import asyncio
import os
import json
from dotenv import load_dotenv
from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from openai import AsyncOpenAI

load_dotenv()

OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "qwen/qwen3-32b")
MCP_SERVER_URL = "http://127.0.0.1:8000/sse"

# Инструменты, требующие подтверждения
HIGH_RISK_TOOLS = {
    "hdfs_chmod": "Изменение прав доступа может нарушить доступ к файлам",
    "hdfs_chown": "Изменение владельца может нарушить права доступа",
    "hdfs_snapshot_delete": "Удаление snapshot необратимо",
    "hdfs_upload": "Файл может перезаписать существующие данные",
}


class MCPAgent:
    def __init__(self, require_confirmation=True):
        self.session = None
        self.tools = []
        self.client = AsyncOpenAI(
            base_url=OPENAI_BASE_URL,
            api_key=OPENAI_API_KEY
        )
        self.require_confirmation = require_confirmation

    def _needs_confirmation(self, tool_name):
        if tool_name in HIGH_RISK_TOOLS:
            return "high"
        return None

    def _ask_confirmation(self, tool_name, func_args):
        risk_level = self._needs_confirmation(tool_name)
        if not risk_level:
            return True

        reason = HIGH_RISK_TOOLS.get(tool_name)
        
        print(f"\n⚠️  ОПАСНАЯ ОПЕРАЦИЯ")
        print(f"Инструмент: {tool_name}")
        print(f"Аргументы: {func_args}")
        print(f"Причина: {reason}")
        
        if self.require_confirmation:
            response = input("\nВыполнить? (y/n): ").strip().lower()
            return response in ['y', 'yes', 'да']
        
        return True

    async def process_request(self, user_message):
        tools = self._convert_tools_for_openai()
        tool_names = [t["function"]["name"] for t in tools]

        response = await self.client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
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
            ],
            tools=tools,
            tool_choice="auto"
        )

        message = response.choices[0].message

        if message.tool_calls:
            tool_call = message.tool_calls[0]
            func_name = tool_call.function.name

            try:
                func_args = json.loads(tool_call.function.arguments)
            except:
                func_args = eval(tool_call.function.arguments)

            # Проверка подтверждения
            if not self._ask_confirmation(func_name, func_args):
                return "ОПЕРАЦИЯ ОТМЕНЕНА\n\nПользователь не подтвердил выполнение операции."

            print(f"Вызов инструмента: {func_name}")
            print(f"Аргументы: {func_args}")

            result = await self.session.call_tool(func_name, arguments=func_args)
            tool_result = result.content[0].text

            # Формируем отчет
            report = f"ОПЕРАЦИЯ ВЫПОЛНЕНА\n\nИнструмент: {func_name}\nРезультат:\n{tool_result}"
            
            return report
        else:
            # Если инструменты не нужны — возвращаем ответ LLM
            return message.content

    def _convert_tools_for_openai(self):
        openai_tools = []
        for tool in self.tools:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.inputSchema
                }
            })
        return openai_tools


async def main():
    print("=" * 60)
    print("MCP AI Агент (OpenAI-совместимый)")
    print("=" * 60)
    print(f"API: {OPENAI_BASE_URL}")
    print(f"Модель: {OPENAI_MODEL}\n")

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