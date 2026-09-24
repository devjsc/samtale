from samtale import Agent


temperature = Agent("temperature")


@temperature.on("read")
async def read(message):
    return {"value": 18.4, "unit": "c"}


if __name__ == "__main__":
    temperature.run(port=8001)
