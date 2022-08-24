from datetime import datetime, timezone

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.exceptions import ApiAttributeError
from datadog_api_client.v2.api.rum_api import RUMApi

PAGE_SIZE = 100
DEFAULT_ATTRIBUTE_MAPPINGS = {
    'app_env': 'attributes.tags.env',
    'app_version': 'attributes.tags.version',
    'browser_name': 'attributes.attributes.browser.name',
    'browser_reload_required': 'attributes.attributes.context.browser_reload_required',
    'browser_useragent': 'attributes.attributes.session.useragent',
    'browser_version_major': 'attributes.attributes.browser.version_major',
    'browser_version': 'attributes.attributes.browser.version',
    'device_brand': 'attributes.attributes.device.brand',
    'device_model': 'attributes.attributes.device.model',
    'device_name': 'attributes.attributes.device.name',
    'device_type': 'attributes.attributes.device.type',
    'error_file': 'attributes.attributes.error.file',
    'error_fingerprint': 'attributes.attributes.error.fingerprint',
    'error_id': 'attributes.attributes.error.id',
    'error_message': 'attributes.attributes.error.message',
    'error_type': 'attributes.attributes.error.type',
    'event_id': 'id',
    'event_timestamp': 'attributes.timestamp',
    'event_type': 'attributes.attributes.type',
    'issue_age': 'attributes.attributes.issue.age',
    'issue_first_seen': 'issue_first_seen',
    'issue_id': 'attributes.attributes.issue.id',
    'os_name': 'attributes.attributes.os.name',
    'os_version_major': 'attributes.attributes.os.version_major',
    'os_version': 'attributes.attributes.os.version',
    'referrer': 'attributes.attributes.view.referrer',
    'rum_application_id': 'attributes.attributes.application.id',
    'rum_session_id': 'attributes.attributes.session.id',
    'service': 'attributes.service',
    'user_id': 'attributes.attributes.usr.id',
    'view_id': 'attributes.attributes.view.id',
    'view_name': 'attributes.attributes.view.name',
    'view_referrer': 'attributes.attributes.view.referrer',
    'view_url_host': 'attributes.attributes.view.url_host',
    'view_url_path_group': 'attributes.attributes.view.url_path_group',
    'view_url_path': 'attributes.attributes.view.url_path',
    'view_url_scheme': 'attributes.attributes.view.url_scheme',
    'view_url_scheme': 'attributes.attributes.view.url_scheme',
}


def get_nested_attr(dict, key):
    try:
        if "." in key:
            key, rest = key.split(".", 1)
            return get_nested_attr(dict[key], rest)
        else:
            val = dict[key]
            if type(val) == datetime:
                return val.isoformat()
            else:
                return val
    except KeyError:
        return None


# Convert tags from key:value to a dict so they can be accessed like other attributes
def tags_to_dict(tags):
    dict = {}
    for tag in tags:
        key, val = tag.split(':', 1)
        dict[key] = val
    return dict


def format_event(event, config_attribute_mappings):
    event_dict = event.to_dict()
    event_dict['attributes']['tags'] = tags_to_dict(event_dict['attributes']['tags'])

    # Convert issue_first_seen value from numeric timestamp to ISO8601
    issue_first_seen_ts = get_nested_attr(event_dict, 'attributes.attributes.issue.first_seen')
    if issue_first_seen_ts:
        dt = datetime.fromtimestamp(issue_first_seen_ts / 1000, tz=timezone.utc)
        event_dict['issue_first_seen'] = dt.isoformat()

    attribute_mappings = {**DEFAULT_ATTRIBUTE_MAPPINGS, **config_attribute_mappings}
    formatted = {}
    for dest_key, source_key in attribute_mappings.items():
        formatted[dest_key] = get_nested_attr(event_dict, source_key)
    return formatted


def format_events(events, config_attribute_mappings):
    return list(map(lambda evt: format_event(evt, config_attribute_mappings), events))


class RUMApiClient:
    def __init__(self, api_key, app_key, start_date):
        self.configuration = Configuration()
        self.configuration.api_key["apiKeyAuth"] = api_key
        self.configuration.api_key["appKeyAuth"] = app_key
        self.start_date = start_date

    def fetch_events(self, query, config_attribute_mappings, cursor = None, newest_first = False):
        with ApiClient(self.configuration) as api_client:
            api_instance = RUMApi(api_client)
            params = {
                'filter_query': query,
                'filter_from': self.start_date,
                'page_limit': PAGE_SIZE,
                'sort': '-timestamp' if newest_first else 'timestamp'
            }
            if cursor:
                params['page_cursor'] = cursor

            response = api_instance.list_rum_events(**params)
            formatted = format_events(response.data, config_attribute_mappings)

            try:
                next_cursor = response.meta.page.after
            except ApiAttributeError:
                next_cursor = None

            return formatted, next_cursor
