from jinja2 import Template
import collections


def render_template(attr, content):
    """
    Renders a template from a field. If the field is a string, it will
    simply render the string and return the result. If it is a collection or
    nested set of collections, it will traverse the structure and render
    all strings in it.
    :param attr: Field to be templated
    :type attr: sequence
    :param content: Content to be rendered
    :type content: dict
    """
    if isinstance(attr, str):
        result = Template(attr).render(content)
    elif isinstance(attr, (list, tuple)):
        result = [Template(e).render(content) if isinstance(e, str) else e for e in attr]
    elif isinstance(attr, dict):
        result = {k: Template(v).render(content) if isinstance(v, str) else v for k, v in attr.items()}
    else:
        result = attr
    return result


def dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], collections.Mapping)):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]
