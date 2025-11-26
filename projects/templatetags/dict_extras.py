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