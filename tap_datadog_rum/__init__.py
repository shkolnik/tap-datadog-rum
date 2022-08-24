#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import dateutil

from tap_datadog_rum.api_client import RUMApiClient
from tap_datadog_rum.schema_builder import SchemaBuilderWithDateSupport

REQUIRED_CONFIG_KEYS = ["api_key", "app_key", "start_date"]
MAX_EVENTS_FOR_SCHEMA_INFERENCE = 3000
LOGGER = singer.get_logger()

def generate_schema(client, query, config_attribute_mapping, start_cursor = None):
    builder = SchemaBuilderWithDateSupport()
    event_count = 0
    next_cursor = start_cursor

    while event_count == 0 or (event_count < MAX_EVENTS_FOR_SCHEMA_INFERENCE and len(events) > 0):
        events, next_cursor = client.fetch_events(query, config_attribute_mapping, next_cursor, True)
        for event in events:
            builder.add_object(event)
        event_count = event_count + len(events)

    return Schema.from_dict(builder.to_schema())

def generate_all_schemas(client, config, state):
    schemas = {}
    for stream_id, stream_config in config['streams'].items():
        query = stream_config['query']
        config_attribute_mapping = stream_config.get('attribute_mapping') or {}
        start_cursor = state.get(stream_id)
        schemas[stream_id] = generate_schema(client, query, config_attribute_mapping, start_cursor)
    return schemas

def schemas_to_catalog(schemas):
    streams = []
    for stream_id, schema in schemas.items():
        stream_metadata = [{
            'metadata': {
                'selected': True,
                'replication-method': 'INCREMENTAL',
            },
            'breadcrumb': []
        }]
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=['event_id'],
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method='INCREMENTAL',
            )
        )
    return Catalog(streams)

def discover(client, config, state):
    all_schemas = generate_all_schemas(client, config, state)
    return schemas_to_catalog(all_schemas)


def sync(client, config, state, catalog):
    """ Sync data from tap source """

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        stream_config = config['streams'][stream.tap_stream_id]
        query = stream_config['query']
        config_attribute_mapping = stream_config.get('attribute_mapping') or {}
        state_cursor = state.get(stream.tap_stream_id)

        events, next_cursor = client.fetch_events(query, config_attribute_mapping, state_cursor)
        while len(events) > 0:
            singer.write_records(stream.tap_stream_id, events)

            state[stream.tap_stream_id] = next_cursor
            singer.write_state(state)

            events, next_cursor = client.fetch_events(query, config_attribute_mapping, next_cursor)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    api_key = args.config['api_key']
    app_key = args.config['app_key']
    start_date = dateutil.parser.parse(args.config['start_date'])
    client = RUMApiClient(api_key, app_key, start_date)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(client, args.config, args.state)
        catalog.dump()
        print('')
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(client, args.config, args.state)
        sync(client, args.config, args.state, catalog)


if __name__ == "__main__":
    main()
