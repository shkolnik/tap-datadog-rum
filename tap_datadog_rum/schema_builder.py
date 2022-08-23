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

# Consolidate nullable type definitions from the more verbose default form:
#       {'anyOf': [{'type': ['null', 'string'], 'format': 'date-time'}]}
# to a more concise form:
#       {"format": "date-time", "type": ["null", "string"]}
def abbreviate_nullable_types(properties):
    for property in properties:
        any_of = properties[property].get('anyOf')
        if any_of is None or not type(any_of) is list:
            continue
        if len(any_of) != 2:
            continue
        if not {'type': 'null'} in any_of:
            continue

        # Delete the null type definition and the real one is left
        del any_of[any_of.index({'type': 'null'})]
        real_def = any_of[0]

        # Ensure type attribute is a list
        if not type(real_def.get('type')) == list:
            real_def['type'] = [real_def.get('type')]

        real_def['type'].append('null')
        real_def['type'].sort()
        properties[property] = real_def

class SchemaBuilderWithDateSupport(SchemaBuilder):
    """ detects & labels date-time formatted strings """
    EXTRA_STRATEGIES = (CustomDateTime, )

    def to_schema(self):
        schema = super().to_schema()
        abbreviate_nullable_types(schema['properties'])
        return schema
