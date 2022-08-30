from datetime import datetime, timezone
from dateutil import parser
from singer.schema import Schema
import re

DEFAULT_ATTRIBUTE_MAPPINGS = {
    'app_env': ('string', 'attributes.tags.env'),
    'app_version': ('string', 'attributes.tags.version'),
    'browser_name': ('string', 'attributes.attributes.browser.name'),
    'browser_useragent': ('string', 'attributes.attributes.session.useragent'),
    'browser_version_major': ('string', 'attributes.attributes.browser.version_major'),
    'browser_version': ('string', 'attributes.attributes.browser.version'),
    'device_brand': ('string', 'attributes.attributes.device.brand'),
    'device_model': ('string', 'attributes.attributes.device.model'),
    'device_name': ('string', 'attributes.attributes.device.name'),
    'device_type': ('string', 'attributes.attributes.device.type'),
    'error_file': ('string', 'attributes.attributes.error.file'),
    'error_fingerprint': ('string', 'attributes.attributes.error.fingerprint'),
    'error_id': ('string', 'attributes.attributes.error.id'),
    'error_message': ('string', 'attributes.attributes.error.message'),
    'error_type': ('string', 'attributes.attributes.error.type'),
    'event_id': ('string', 'id'),
    'event_timestamp': ('datetime', 'attributes.timestamp'),
    'event_type': ('string', 'attributes.attributes.type'),
    'issue_age_ns': ('integer', 'attributes.attributes.issue.age'),
    'issue_first_seen': ('datetime', 'attributes.attributes.issue.first_seen'),
    'issue_id': ('string', 'attributes.attributes.issue.id'),
    'os_name': ('string', 'attributes.attributes.os.name'),
    'os_version_major': ('string', 'attributes.attributes.os.version_major'),
    'os_version': ('string', 'attributes.attributes.os.version'),
    'rum_application_id': ('string', 'attributes.attributes.application.id'),
    'rum_session_id': ('string', 'attributes.attributes.session.id'),
    'service': ('string', 'attributes.service'),
    'user_id': ('string', 'attributes.attributes.usr.id'),
    'view_id': ('string', 'attributes.attributes.view.id'),
    'view_name': ('string', 'attributes.attributes.view.name'),
    'view_referrer': ('string', 'attributes.attributes.view.referrer'),
    'view_url_host': ('string', 'attributes.attributes.view.url_host'),
    'view_url_path_group': ('string', 'attributes.attributes.view.url_path_group'),
    'view_url_path': ('string', 'attributes.attributes.view.url_path'),
    'view_url_scheme': ('string', 'attributes.attributes.view.url_scheme'),
}

def cooerce_string(val):
  if val is None:
    return None

  if type(val) == datetime:
    return cooerce_datetime(val)

  return str(val)

def cooerce_int(val):
  if type(val) == int:
    return val

  if type(val) == bool:
    return int(val)

  if type(val) == str:
    if val.endswith('.0'):
      val = val[:-2]

    if re.match("^-?\d+$", val):
      return int(val)

  return None

def cooerce_float(val):
  if type(val) == float:
    return val

  if type(val) == int:
    return float(val)

  if type(val) == str:
    try:
      return float(val)
    except ValueError:
      return None

  return None

def cooerce_bool(val):
  if type(val) == bool:
    return val

  if type(val) == int and (val == 0 or val == 1):
    return bool(val)

  if type(val) == str:
    val = val.lower()
    if val in ['true', 't', '1']:
      return True
    if val in ['false', 'f', '0']:
      return False

  return None

def cooerce_datetime(val):
  if type(val) == datetime:
    return val.isoformat()

  if type(val) == str:
    try:
      dt = parser.parse(val)
      return dt.isoformat()
    except:
      return None

  if type(val) == int: # DD emits datetimes as timestamp nanoseconds from epoch
    dt = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
    return dt.isoformat()

  return None

VALUE_COOERCERS = {
  'string': cooerce_string,
  'integer': cooerce_int,
  'boolean': cooerce_bool,
  'number': cooerce_float,
  'datetime': cooerce_datetime
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

def format_event(event, attribute_mappings):
    event_dict = event.to_dict()
    event_dict['attributes']['tags'] = tags_to_dict(event_dict['attributes']['tags'])

    # Convert issue_first_seen value from numeric timestamp to ISO8601
    # issue_first_seen_ts = get_nested_attr(event_dict, 'attributes.attributes.issue.first_seen')
    # if issue_first_seen_ts:
    #     dt = datetime.fromtimestamp(issue_first_seen_ts / 1000, tz=timezone.utc)
    #     event_dict['issue_first_seen'] = dt.isoformat()

    formatted = {}
    for attr_name, (attr_type, attr_path) in attribute_mappings.items():
        val = get_nested_attr(event_dict, attr_path)

        cooercer_func = VALUE_COOERCERS.get(attr_type)
        if cooercer_func is None:
          raise("Don't know how to handle attribute type {attr_type}")

        formatted[attr_name] = cooercer_func(val)

    return formatted

class SchemaMapper:
    def __init__(self, attribute_mappings):
      self.attribute_mappings = {**DEFAULT_ATTRIBUTE_MAPPINGS, **attribute_mappings}

    def map_events(self, events):
      return list(map(lambda evt: format_event(evt, self.attribute_mappings), events))

    def to_schema(self):
      properties = {}
      for attr_name, (attr_type, attr_path) in self.attribute_mappings.items():
        if attr_type == 'datetime':
          property = {
            'type': ['null', 'string'],
            'format': 'date-time'
          }
        else:
          property = {'type': sorted([attr_type, 'null'])}
        properties[attr_name] = property
      return Schema.from_dict({'properties': properties, 'type': 'object'})
