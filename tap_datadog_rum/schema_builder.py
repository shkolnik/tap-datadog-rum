from datetime import datetime
from genson import TypedSchemaStrategy, SchemaBuilder

DATE_FORMAT_WITH_MILLIS = "%Y-%m-%dT%H:%M:%S.%f%z"
DATE_FORMAT_NO_MILLIS = "%Y-%m-%dT%H:%M:%S%z"

def is_formatted_date(str, format):
    try:
        date_time_obj = datetime.strptime(str, format)
        if isinstance(date_time_obj, datetime):
            return True
        else:
            return False
    except (TypeError, ValueError) as exception:
        return False

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
        if type(obj) != str:
          return False

        return is_formatted_date(obj, DATE_FORMAT_WITH_MILLIS) or \
                is_formatted_date(obj, DATE_FORMAT_NO_MILLIS)

    def to_schema(self):
        schema = super().to_schema()
        schema['type'] = self.JS_TYPE
        schema['format'] = self.format
        return schema


class SchemaBuilderWithDateSupport(SchemaBuilder):
    """ detects & labels date-time formatted strings """
    EXTRA_STRATEGIES = (CustomDateTime, )
