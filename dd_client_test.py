import sys
import json
from time import sleep
from datetime import datetime

from singer.schema import Schema

from tap_datadog_rum.api_client import RUMApiClient
from tap_datadog_rum.schema_builder import generate_schema_from_events

client = RUMApiClient("", "")

query = '@context.browser_reload_required:true env:production service:zenpayroll'

def fetch_events(query, start_date, start_cursor = None):
  next_cursor = start_cursor
  for x in range(30):
    print("Loop #" + str(x))
    events, response_cursor = client.fetch_events(query, start_date, next_cursor)

    if len(events) == 0:
      print('No more events right now. Waiting.')
      sleep(5)
    else:
      next_cursor = response_cursor
      for event in events:
        print(json.dumps(event, indent=4, sort_keys=True))
      print("Cursor: " + str(next_cursor))

    if next_cursor is None:
      print('finished')
      sys.exit()

    sleep(1)


def generate_schema(query, start_date):
  events, _cursor = client.fetch_events(query, start_date)
  return Schema.from_dict(generate_schema_from_events(events))
  print(builder.to_json(indent=2))


start_date = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)

schema = generate_schema(query, start_date)
print(json.dumps(schema.to_dict(), indent = 2))

# fetch_events(query, start_date)
