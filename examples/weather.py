from typing import Literal
from pydantic import BaseModel, Field
from chatters import Agent, Message


class WeatherRequest(BaseModel):
    location: str = Field(min_length=1)
    unit: Literal["c", "f"] = "c"

weather = Agent("weather", max_concurrency=4)

@weather.on("weather_request", model=WeatherRequest)
async def get_weather(message: Message, request: WeatherRequest):
    temperature = 18.4 if request.unit == "c" else 65.1
    return {
        "location": request.location,
        "temperature": temperature,
        "unit": request.unit,
    }

if __name__ == "__main__":
    weather.run(port=8003)
