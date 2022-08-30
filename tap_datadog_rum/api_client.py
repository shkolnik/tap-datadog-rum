from time import sleep
import sys

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.exceptions import ApiAttributeError, ApiException
from datadog_api_client.v2.api.rum_api import RUMApi

PAGE_SIZE = 100
MAX_RETRIES = 5

def list_events_with_retries(client, params):
    api_instance = RUMApi(client)
    for _prev_tries in range(0, MAX_RETRIES):
        try:
            return api_instance.list_rum_events(**params)
        except ApiException as err:
            my_err = err
            if err.status != 429: # Datadog returns 429 for rate limit
                raise err # Re-raise anything other than a rate limit

            try:
                rate_limit_reset_sec = int(err.headers['x-ratelimit-reset'])
            except (KeyError, ValueError):
                rate_limit_reset_sec = 30

            print(f'Datadog rate limit reached, sleeping for {rate_limit_reset_sec}s', file=sys.stderr)
            sleep(rate_limit_reset_sec)

    raise 'Still hitting Datadog rate limit after all retries exhausted'

class RUMApiClient:
    def __init__(self, api_key, app_key, start_date):
        self.configuration = Configuration()
        self.configuration.api_key["apiKeyAuth"] = api_key
        self.configuration.api_key["appKeyAuth"] = app_key
        self.start_date = start_date

    def fetch_events(self, query, config_attribute_mappings, cursor = None, newest_first = False):
        with ApiClient(self.configuration) as api_client:
            params = {
                'filter_query': query,
                'filter_from': self.start_date,
                'page_limit': PAGE_SIZE,
                'sort': '-timestamp' if newest_first else 'timestamp'
            }
            if cursor:
                params['page_cursor'] = cursor

            response = list_events_with_retries(api_client, params)

            try:
                next_cursor = response.meta.page.after
            except ApiAttributeError:
                next_cursor = None

            return response.data, next_cursor
