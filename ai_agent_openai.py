import asyncio
import os
import json
import time
import logging
from dotenv import load_dotenv
from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from openai import AsyncOpenAI, APIConnectionError, APIStatusError, APITimeoutError

load_dotenv()

OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "qwen/qwen3-32b")
MCP_SERVER_URL = "http://127.0.0.1:8000/sse"

RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "1.0"))      
RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", "30.0"))        
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))
REQUEST_DEADLINE = float(os.getenv("REQUEST_DEADLINE", "120.0"))      
OPENAI_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "60.0"))           
MCP_TOOL_TIMEOUT = float(os.getenv("MCP_TOOL_TIMEOUT", "60.0"))  

HIGH_RISK_TOOLS = {
    "hdfs_chmod": "Изменение прав доступа может нарушить доступ к файлам",
    "hdfs_chown": "Изменение владельца может нарушить права доступа",
    "hdfs_snapshot_delete": "Удаление snapshot необратимо",
    "hdfs_upload": "Файл может перезаписать существующие данные",
}

AGENT_LOG_FILE = os.getenv("AGENT_LOG_FILE", "mcp_agent.log")

OPENAI_RETRIABLE = (APIConnectionError, APITimeoutError)

logger = logging.getLogger("mcp_agent")
logger.setLevel(logging.INFO)
logger.propagate = False

_agent_handler = logging.FileHandler(AGENT_LOG_FILE, encoding="utf-8")
_agent_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(_agent_handler)


class RetryExhaustedError(Exception):
    pass


class DeadlineExceededError(Exception):
    pass


async def async_retry_with_backoff(
    coro_fn,
    *args,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
    max_delay: float = RETRY_MAX_DELAY,
    backoff_factor: float = RETRY_BACKOFF_FACTOR,
    deadline: float = None,               
    retriable_exceptions: tuple = (Exception,),
    operation_name: str = "operation",
    **kwargs,
):
    """
    Асинхронный retry с экспоненциальной задержкой и поддержкой дедлайна.

    Args:
        coro_fn:               async callable
        max_attempts:          максимум попыток
        base_delay:            начальная задержка
        max_delay:             максимальная задержка
        backoff_factor:        множитель задержки
        deadline:              абсолютный monotonic timestamp крайнего срока
        retriable_exceptions:  типы исключений для retry
        operation_name:        имя для логов
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        if deadline is not None and time.monotonic() >= deadline:
            raise DeadlineExceededError(
                f"Дедлайн превышен до попытки {attempt} для '{operation_name}'"
            )

        try:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DeadlineExceededError(f"Дедлайн истёк для '{operation_name}'")
                return await asyncio.wait_for(coro_fn(*args, **kwargs), timeout=remaining)
            else:
                return await coro_fn(*args, **kwargs)

        except asyncio.TimeoutError as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
            logger.warning(
                f"[retry] '{operation_name}' попытка {attempt}/{max_attempts}: "
                f"таймаут. Следующая через {delay:.2f}s"
            )
            await asyncio.sleep(delay)

        except retriable_exceptions as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DeadlineExceededError(
                        f"Дедлайн превышен после попытки {attempt} для '{operation_name}'"
                    ) from exc
                delay = min(delay, remaining)
            logger.warning(
                f"[retry] '{operation_name}' попытка {attempt}/{max_attempts}: "
                f"{exc}. Следующая через {delay:.2f}s"
            )
            await asyncio.sleep(delay)

    raise RetryExhaustedError(
        f"'{operation_name}' не выполнена за {max_attempts} попыток. "
        f"Последняя ошибка: {last_exc}"
    ) from last_exc

class MCPAgent:
    def __init__(self, require_confirmation=True):
        self.session = None
        self.tools = []
        self.client = AsyncOpenAI(
            base_url=OPENAI_BASE_URL,
            api_key=OPENAI_API_KEY,
            timeout=OPENAI_TIMEOUT,   
        )
        self.require_confirmation = require_confirmation

    def _needs_confirmation(self, tool_name):
        return "high" if tool_name in HIGH_RISK_TOOLS else None

    def _ask_confirmation(self, tool_name, func_args):
        if not self._needs_confirmation(tool_name):
            return True

        reason = HIGH_RISK_TOOLS.get(tool_name)
        print(f"ОПАСНАЯ ОПЕРАЦИЯ")
        print(f"Инструмент: {tool_name}")
        print(f"Аргументы: {func_args}")
        print(f"Причина: {reason}")

        if self.require_confirmation:
            response = input("\nВыполнить? (y/n): ").strip().lower()
            return response in ['y', 'yes', 'да']
        return True

    async def _call_openai(self, messages, tools):
        """Вызов OpenAI API с retry и таймаутом."""
        async def _do():
            return await self.client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=messages,
                tools=tools,
                tool_choice="auto",
            )

        return await async_retry_with_backoff(
            _do,
            max_attempts=RETRY_MAX_ATTEMPTS,
            retriable_exceptions=OPENAI_RETRIABLE,
            operation_name="openai_chat_completion",
        )

    async def _call_mcp_tool(self, func_name, func_args, deadline: float = None):
        """Вызов MCP tool с retry, таймаутом и поддержкой дедлайна."""
        async def _do():
            return await asyncio.wait_for(
                self.session.call_tool(func_name, arguments=func_args),
                timeout=MCP_TOOL_TIMEOUT,
            )

        return await async_retry_with_backoff(
            _do,
            max_attempts=RETRY_MAX_ATTEMPTS,
            retriable_exceptions=(Exception,),
            deadline=deadline,
            operation_name=f"mcp_tool:{func_name}",
        )

    async def process_request(self, user_message):
        """
        Обрабатывает запрос пользователя.
        Весь запрос ограничен дедлайном REQUEST_DEADLINE секунд.
        """
        deadline = time.monotonic() + REQUEST_DEADLINE

        tools = self._convert_tools_for_openai()
        tool_names = [t["function"]["name"] for t in tools]

        messages = [
            {
                "role": "system",
                "content": (
                    "Ты — AI-ассистент для управления HDFS кластером через MCP инструменты.\n\n"
                    "ОГРАНИЧЕНИЯ:\n"
                    f"- У тебя есть строгий список доступных команд: {', '.join(tool_names)}\n"
                    "- Ты НЕ можешь выполнять команды, которых нет в этом списке\n"
                    "- Если запрос не соответствует доступным инструментам — честно скажи, что не можешь это сделать\n"
                    "- Не выдумывай команды и не обещай выполнить то, что недоступно\n\n"
                    "ТРЕБОВАНИЯ К ОТВЕТУ:\n"
                    "- После выполнения любой операции выводи краткий отчет: что сделано, какой результат\n"
                    "- Если были ошибки — сообщи о них четко\n"
                    "- Для сложных операций (загрузка, snapshot, квоты) — пиши план: что было → что стало\n\n"
                    "БЕЗОПАСНОСТЬ:\n"
                    "- Некоторые операции требуют подтверждения пользователя\n"
                    "- Если операция опасная — запроси подтверждение перед выполнением"
                )
            },
            {"role": "user", "content": user_message}
        ]

        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return "ОШИБКА: дедлайн запроса истёк до отправки в LLM."

            response = await asyncio.wait_for(
                self._call_openai(messages, tools),
                timeout=remaining,
            )
        except asyncio.TimeoutError:
            return f"ОШИБКА: дедлайн запроса ({REQUEST_DEADLINE}s) истёк при обращении к LLM."
        except RetryExhaustedError as e:
            return f"ОШИБКА: не удалось получить ответ от LLM после {RETRY_MAX_ATTEMPTS} попыток. {e}"

        message = response.choices[0].message

        if not message.tool_calls:
            return message.content

        tool_call = message.tool_calls[0]
        func_name = tool_call.function.name

        try:
            func_args = json.loads(tool_call.function.arguments)
        except Exception:
            func_args = eval(tool_call.function.arguments)

        if not self._ask_confirmation(func_name, func_args):
            return "ОПЕРАЦИЯ ОТМЕНЕНА\n\nПользователь не подтвердил выполнение операции."

        print(f"Вызов инструмента: {func_name}")
        print(f"Аргументы: {func_args}")

        try:
            result = await self._call_mcp_tool(func_name, func_args, deadline=deadline)
        except DeadlineExceededError as e:
            return f"ОШИБКА: дедлайн запроса истёк при выполнении инструмента '{func_name}'. {e}"
        except RetryExhaustedError as e:
            return (
                f"ОШИБКА: инструмент '{func_name}' не выполнен за {RETRY_MAX_ATTEMPTS} попытки. {e}"
            )
        except Exception as e:
            return f"ОШИБКА при вызове инструмента '{func_name}': {e}"

        tool_result = result.content[0].text
        return f"ОПЕРАЦИЯ ВЫПОЛНЕНА\n\nИнструмент: {func_name}\nРезультат:\n{tool_result}"

    def _convert_tools_for_openai(self):
        return [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.inputSchema,
                }
            }
            for tool in self.tools
        ]

async def main():
    print("=" * 60)
    print("MCP AI Агент (OpenAI-совместимый)")
    print("=" * 60)
    print(f"API: {OPENAI_BASE_URL}")
    print(f"Модель: {OPENAI_MODEL}")
    print(f"Retry: max={RETRY_MAX_ATTEMPTS}, base_delay={RETRY_BASE_DELAY}s, "
          f"factor={RETRY_BACKOFF_FACTOR}x, max_delay={RETRY_MAX_DELAY}s")
    print(f"Дедлайн запроса: {REQUEST_DEADLINE}s | "
          f"Таймаут OpenAI: {OPENAI_TIMEOUT}s | "
          f"Таймаут MCP tool: {MCP_TOOL_TIMEOUT}s\n")

    agent = MCPAgent()

    async with sse_client(MCP_SERVER_URL) as (read, write):
        async with ClientSession(read, write) as session:
            agent.session = session

            print("Инициализация MCP сессии...")
            await session.initialize()

            tools_response = await session.list_tools()
            agent.tools = tools_response.tools

            print(f"Загружено инструментов: {len(agent.tools)}")
            print(f"Инструменты: {[t.name for t in agent.tools]}")
            print("\nГотов к работе (quit для выхода)\n")

            while True:
                try:
                    user_input = input("Вы: ").strip()
                    if not user_input:
                        continue
                    if user_input.lower() in ['quit', 'exit', 'q']:
                        print("Выход...")
                        break

                    print("Обработка...", end="\r")
                    response = await agent.process_request(user_input)
                    print(" " * 20, end="\r")
                    print(f"Ответ:\n{response}\n")

                except KeyboardInterrupt:
                    print("\nВыход...")
                    break
                except Exception as e:
                    print(f"Ошибка: {e}\n")


if __name__ == "__main__":
    asyncio.run(main())