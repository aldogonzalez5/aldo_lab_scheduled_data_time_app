from datetime import datetime
import statistics

from service.constants import DATASET_PROVIDER, DATASET_NAME
from corva import Api, Cache, Logger, ScheduledDataTimeEvent, scheduled


@scheduled
def lambda_handler(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
# 3. Here is where you can declare your variables from the argument event: ScheduledDataTimeEvent and start using Api, Cache and Logger functionalities. 

    # The scheduled app can declare the following attributes from the ScheduledDataTimeEvent: company_id: The company identifier; asset_id: The asset identifier; start_time: The start time of interval; end_time: The end time of interval
    asset_id = event.asset_id
    company_id = event.company_id
    start_time = event.start_time
    end_time = event.end_time

# 4. Utilize the attributes from the ScheduledDataTimeEvent to make an API request to corva#wits or any desired time type dataset.
    # You have to fetch the realtime drilling data for the asset based on start and end time of the event.
    # start_time and end_time are inclusive so the query is structured accordingly to avoid processing duplicate data
    # We are only querying for rop field since that is the only field we need. It is nested under data. We are using the SDK convenience method api.get_dataset. See API Section for more information on convenience method. 
    records = api.get_dataset(
        provider="corva",
        dataset= "wits",
        query={
            'asset_id': asset_id,
            'timestamp': {
                '$gte': start_time,
                '$lte': end_time,
            }
        },
        sort={'timestamp': 1},
        limit=500,
        fields="data.rop, timestamp"
    )
    Logger.debug(f"start_time: {start_time}")
    Logger.debug(f"end_time: {end_time}")
    Logger.debug(f"asset_id: {asset_id}")
    Logger.debug(f"default_headers: {api.default_headers}")
    Logger.debug(f"api_url: {api.api_url}")
    Logger.debug(f"data_api_url: {api.data_api_url}")
    Logger.debug(f"api_key: {api.api_key}")


    record_count = len(records)

    # Utilize the Logger functionality. The default log level is Logger.info. To use a different log level, the log level must be specified in the manifest.json file in the "settings.environment": {"LOG_LEVEL": "DEBUG"}. See the Logger documentation for more information.
    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{start_time=} {end_time=} {record_count=}")


# 5. Implementing some calculations
    # Computing mean rop value from the list of realtime wits records
    rop = statistics.mean(record.get("data", {}).get("rop", 0) for record in records)

    # Utililize the Cache functionality to get a set key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information.
    # Getting last exported timestamp from Cache 
    last_exported_timestamp = int(cache.get(key='last_exported_timestamp') or 0)

    # Making sure we are not processing duplicate data
    if end_time <= last_exported_timestamp:
        Logger.debug(f"Already processed data until {last_exported_timestamp=}")
        return None

# 6. This is how to set up a body of a POST request to store the mean rop data and the start_time and end_time of the interval from the event.
    
    output = {
        "timestamp": records[-1].get("timestamp"),
        "asset_id": asset_id,
        "company_id": company_id,
        "provider": DATASET_PROVIDER,
        "collection": DATASET_NAME,
        "data": {
            "rop": rop,
            "start_time": start_time,
            "end_time": end_time
        },
        "version": 1
    }

    # Utilize the Logger functionality.
    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{start_time=} {end_time=} {record_count=}")
    Logger.debug(f"{output=}")

# 7. Save the newly calculated data in a custom dataset
    try:
        # Utilize the Api functionality. The data=outputs needs to be an an array because Corva's data is saved as an array of objects. Objects being records. See the Api documentation for more information.
        response = api.post(
            f"api/v1/data/{DATASET_PROVIDER}/{DATASET_NAME}/", data=[output],
        )
        response.raise_for_status()

    # Utililize the Cache functionality to set a key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information. This example is setting the last timestamp of the output to Cache
        cache.set(key='last_exported_timestamp', value=records[-1].get("timestamp"))
    except Exception as ex:
        Logger.debug(f"{response.text}")
        Logger.debug(f"error: {str(ex)}")
        raise ex
    
    return output
