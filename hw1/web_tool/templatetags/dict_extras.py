from django import template
register = template.Library()

@register.filter
def get_item(d, key):
    """讓模板可以用 row|get_item:col 取 dict 的值"""
    try:
        return d.get(key, "")
    except Exception:
        return ""