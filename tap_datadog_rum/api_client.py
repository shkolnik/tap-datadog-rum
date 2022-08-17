from datetime import datetime

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.exceptions import ApiAttributeError
from datadog_api_client.v2.api.rum_api import RUMApi

PAGE_SIZE = 100

ATTRIBUTE_MAPPING = {
    'app_env': 'attributes.tags.env',
    'app_version': 'attributes.tags.version',
    'browser_name': 'attributes.attributes.browser.name',
    'browser_reload_required': 'attributes.attributes.context.browser_reload_required',
    'browser_useragent': 'attributes.attributes.session.useragent',
    'browser_version_major': 'attributes.attributes.browser.version_major',
    'browser_version': 'attributes.attributes.browser.version',
    'company_id': 'attributes.attributes.context.company_id',
    'device_name': 'attributes.attributes.device.name',
    'device_type': 'attributes.attributes.device.type',
    'error_file': 'attributes.attributes.error.file',
    'error_fingerprint': 'attributes.attributes.error.fingerprint',
    'error_id': 'attributes.attributes.error.id',
    'error_message': 'attributes.attributes.error.message',
    'error_presentation_style': 'attributes.attributes.context.error_presentation_style',
    'error_type': 'attributes.attributes.error.type',
    'event_id': 'id',
    'event_timestamp': 'attributes.timestamp',
    # 'gusto_view_name': 'attributes.attributes.context.gusto_view_name',
    'graphql_operation_name': 'attributes.attributes.context.graphql_operation_name',
    'issue_age': 'attributes.attributes.issue.age',
    'issue_first_seen': 'attributes.attributes.issue.first_seen',
    'issue_id': 'attributes.attributes.issue.id',
    'os_name': 'attributes.attributes.os.name',
    'os_version_major': 'attributes.attributes.os.version_major',
    'os_version': 'attributes.attributes.os.version',
    'referrer': 'attributes.attributes.view.referrer',
    'rum_session_id': 'attributes.attributes.session.id',
    'service': 'attributes.service',
    'team': 'attributes.attributes.context.team',
    'user_2fa_active': 'attributes.attributes.usr.second_factor_active',
    'user_id': 'attributes.attributes.usr.id',
    'user_role_id': 'attributes.attributes.context.user_role_id',
    'user_role_type': 'attributes.attributes.context.user_role_type',
    'view_id': 'attributes.attributes.view.id',
    'view_name': 'attributes.attributes.view.name',
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


def format_event(event):
    event_dict = event.to_dict()
    event_dict['attributes']['tags'] = tags_to_dict(
        event_dict['attributes']['tags'])

    formatted = {}
    for dest_key, source_key in ATTRIBUTE_MAPPING.items():
        formatted[dest_key] = get_nested_attr(event_dict, source_key)
    return formatted


def format_events(events):
    return list(map(lambda evt: format_event(evt), events))


class RUMApiClient:
    def __init__(self, api_key, app_key, start_date):
        self.configuration = Configuration()
        self.configuration.api_key["apiKeyAuth"] = api_key
        self.configuration.api_key["appKeyAuth"] = app_key
        self.start_date = start_date

    def fetch_events(self, query, cursor=None):
        with ApiClient(self.configuration) as api_client:
            api_instance = RUMApi(api_client)
            params = {
                'filter_query': query,
                'filter_from': self.start_date,
                'page_limit': PAGE_SIZE,
                'sort': 'timestamp'
            }
            if cursor:
                params['page_cursor'] = cursor

            response = api_instance.list_rum_events(**params)
            formatted = format_events(response.data)

            try:
                next_cursor = response.meta.page.after
            except ApiAttributeError:
                next_cursor = None

            return formatted, next_cursor
