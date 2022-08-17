from datetime import datetime
from genson import TypedSchemaStrategy, SchemaBuilder


# Checks for strings that look like ISO datetime and includes a date-time format
# on the corresponding properties in the JSON Schema.
class CustomDateTime(TypedSchemaStrategy):
    """
    strategy for date-time formatted strings
    """
    JS_TYPE = 'string'
    PYTHON_TYPE = (str, type(u''))

    # create a new instance variable
    def __init__(self, node_class):
        super().__init__(node_class)
        self.format = "date-time"

    @classmethod
    def match_object(self, obj):
        super().match_object(obj)
        try:
            date_time_obj = datetime.strptime(obj, "%Y-%m-%dT%H:%M:%S.%f%z")
            if isinstance(date_time_obj, datetime):
                return True
            else:
                return False
        except (TypeError, ValueError) as exception:
            # print(exception)
            return False

    def to_schema(self):
        schema = super().to_schema()
        schema['type'] = self.JS_TYPE
        schema['format'] = self.format
        return schema


class SchemaBuilderWithDateSupport(SchemaBuilder):
    """ detects & labels date-time formatted strings """
    EXTRA_STRATEGIES = (CustomDateTime, )


def generate_schema_from_events(events):
    builder = SchemaBuilderWithDateSupport()
    for event in events:
        builder.add_object(event)

    return builder.to_schema()
