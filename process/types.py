# Lint as: python3
"""Shared types for processing a schema-less CSV.
"""
from collections import defaultdict
from collections import namedtuple

Field = namedtuple('Field',
                   ['name', 'value', 'always_treat_as_empty'],
                   defaults=['', '', False])


NameValue = namedtuple('NameValue', ['name', 'value', 'data_source'],
                       defaults=['', '', ''])


# TODO: possible code smell, need a better structure
def four_level_dict():
    return defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(str))))
