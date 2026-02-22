import asyncio
import os
import json
from dotenv import load_dotenv
from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from openai import AsyncOpenAI

load_dotenv()

OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "ollama")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "llama3.2")
MCP_SERVER_URL = "http://127.0.0.1:8000/sse"


class MCPAgent:
    def __init__(self):
        self.session = None
        self.tools = []
        self.client = AsyncOpenAI(
            base_url=OPENAI_BASE_URL,
            api_key=OPENAI_API_KEY
        )

    async def process_request(self, user_message):
        tools = self._convert_tools_for_openai()

        response = await self.client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — AI-ассистент с доступом к инструментам. "
                        "Если запрос требует работы с файлами, данными или вычислений — используй инструменты. "
                        "Если можешь ответить сам — отвечай без инструментов."
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

            print(f"Вызов инструмента: {func_name}")
            print(f"Аргументы: {func_args}")

            result = await self.session.call_tool(func_name, arguments=func_args)
            tool_result = result.content[0].text

            return tool_result
        else:
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