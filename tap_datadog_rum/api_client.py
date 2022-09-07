from time import sleep
import sys

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.exceptions import ApiAttributeError
from datadog_api_client.v2.api.rum_api import RUMApi

DEFAULT_PAGE_SIZE = 1000
MAX_RETRIES = 5

def list_events_with_retries(client, params, logger):
    api_instance = RUMApi(client)
    for _prev_tries in range(0, MAX_RETRIES):
        try:
            return api_instance.list_rum_events(**params)
        except Exception as err:
            my_err = err

            try:
                rate_limit_reset_sec = int(err.headers['x-ratelimit-reset'])
            except (KeyError, ValueError, AttributeError):
                rate_limit_reset_sec = 30

            if hasattr(err, 'status') and err.status == 429: # Datadog returns 429 for rate limit
                logger.info(f'Datadog rate limit reached, sleeping for {rate_limit_reset_sec}s')
            else:
                logger.warn(f'An error occurred while fetching records from Datadog. Retrying in {rate_limit_reset_sec}s.')

            sleep(rate_limit_reset_sec)

    raise 'Still hitting Datadog rate limit after all retries exhausted'

class RUMApiClient:
    def __init__(self, logger, api_key, app_key, start_date, page_size):
        self.logger = logger
        self.configuration = Configuration()
        self.configuration.api_key["apiKeyAuth"] = api_key
        self.configuration.api_key["appKeyAuth"] = app_key
        self.start_date = start_date
        self.page_size = page_size or DEFAULT_PAGE_SIZE

    def fetch_events(self, query, config_attribute_mappings, cursor = None):
        with ApiClient(self.configuration) as api_client:
            params = {
                'filter_query': query,
                'filter_from': self.start_date,
                'page_limit': self.page_size,
                'sort': 'timestamp'
            }
            if cursor:
                params['page_cursor'] = cursor

            response = list_events_with_retries(api_client, params, self.logger)

            try:
                next_cursor = response.meta.page.after
            except ApiAttributeError:
                next_cursor = None

            return response.data, next_cursor
