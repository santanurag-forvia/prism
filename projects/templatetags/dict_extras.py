# projects/templatetags/dict_extras.py
from django import template
register = template.Library()

@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)

@register.filter
def get_by_key_value(items, args):
    """
    Usage: {{ list|get_by_key_value:"key,value" }}
    Returns the first dict in list where dict[key] == value.
    """
    key, value = args.split(',')
    for item in items:
        # Convert both to string for comparison
        if str(item.get(key)) == value:
            return item
    return None

@register.filter
def get_item(dictionary, key):
    if isinstance(dictionary, dict):
        return dictionary.get(key)
    return None

@register.filter
def map_filter(value, arg=None):
    # Example: uppercase and append arg
    if arg:
        return f"{str(value).upper()} {arg}"
    return str(value).upper()

@register.filter
def to_list(value):
    return list(value)