import asyncio
import json

from chatters import Agent


async def main():
    consumer = Agent("consumer")
    try:
        unsupported = await consumer.send(
            "http://localhost:8003",
            "wrong_request",
            location="London",
        )
        print("Unsupported request response:")
        print(json.dumps(unsupported.to_dict(), indent=2))

        weather = await consumer.ask(
            "http://localhost:8003",
            "weather_request",
            location="London",
            unit="c",
        )
        print("\nValid weather response:")
        print(json.dumps(weather, indent=2))
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
