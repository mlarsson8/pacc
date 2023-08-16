import httpx  # requests capability, but can work with async
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=2))
def fetch_temperature(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return most_recent_temp


@task(retries=5)
def fetch_windspeed(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="windspeed_10m"),
    )
    most_recent_ws = float(weather.json()["hourly"]["windspeed_10m"][0])
    return most_recent_ws


@task(retries=1)
def fetch_cloudcover(lat: float, lon: float):
    base_url = "https://xapi.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="cloudcover"),
    )
    most_recent_cloudcover = float(weather.json()["hourly"]["cloudcover"][0])
    return most_recent_cloudcover


@task(retries=2)
def save_weather(data: str):
    with open("weather.csv", "w+") as w:
        w.write(data)
    return "Successfully wrote temp"


@flow(retries=1)
def pipeline(lat: float, lon: float):
    logger = get_run_logger()
    temp = fetch_temperature(lat, lon)
    ws = fetch_windspeed(lat, lon)
    cloudcover = fetch_cloudcover(lat, lon)
    logger.info(f'{temp}, {ws}, {cloudcover}')
    result = save_weather(f'{temp},{ws},{cloudcover}')
    return result


if __name__ == "__main__":
    pipeline(38.9, -77.0)