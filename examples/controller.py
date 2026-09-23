import asyncio

from chatters import Agent


async def main():
    controller = Agent("controller")
    try:
        reading = await controller.ask("http://localhost:8001", "read")
        print(reading)
    finally:
        await controller.close()


if __name__ == "__main__":
    asyncio.run(main())
