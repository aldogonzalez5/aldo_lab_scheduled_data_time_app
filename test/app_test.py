import json
import os

from corva import ScheduledDataTimeEvent
from lambda_function import lambda_handler
from service.constants import DATASET_PROVIDER, DATASET_NAME

TEST_STREAMS_PATH =  os.path.join(os.path.abspath(os.path.dirname(__file__)), 'streams')

class TestScheduledTimeApp:
    def test_app(self, app_runner, requests_mock):
        event = ScheduledDataTimeEvent(
            company_id=1, asset_id=1234, start_time=1578291000, end_time=1578291300
        )
        stream_file = os.path.join(TEST_STREAMS_PATH, "corva_wits.json")
        with open(stream_file, encoding="utf8") as raw_stream:
            corva_wits = json.load(raw_stream)
        requests_mock.get(f'https://data.localhost.ai/api/v1/data/corva/wits/', json=corva_wits)
        requests_mock.post(f'https://data.localhost.ai/api/v1/data/{DATASET_PROVIDER}/{DATASET_NAME}/')
        output = app_runner(lambda_handler, event=event)
        assert output
        assert output["data"]["rop"] == 3
