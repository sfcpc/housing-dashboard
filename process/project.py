# Lint as: python3
"""Contains logic to create a unified view of a 'project.'

Project information can come from multiple data sources with multiple
records, and this contains all the handling to make sense of it all.
"""
from collections import namedtuple
from datetime import datetime


_Record = namedtuple('_Record', ['key', 'values'], defaults=[None, None])


class Project:
    """A way to abstract some of the details of handling multiple records for a
    project, from multiple sources."""

    def __init__(self, id, data):
        self.id = id
        if len(data['ppts']) == 0:
            raise Exception('No implementation to handle non-ppts data yet')

        main = None
        children = []
        main_date = datetime.min
        for (fk, values) in data['ppts'].items():
            if not values['parent']['value']:
                if main is None or (
                        main is not None and
                        values['parent']['last_updated'] > main_date):
                    main = _Record(fk, values)
                    main_date = values['parent']['last_updated']
            else:
                children.append(_Record(fk, values))

        if not main:
            # upgrade the oldest child
            oldest_child_and_date = None
            for child in children:
                oldest_date = datetime.max
                for (name, data) in child.values.items():
                    if data['last_updated'] < oldest_date:
                        oldest_date = data['last_updated']

                if (not oldest_child_and_date or
                        oldest_date < oldest_child_and_date[1]):
                    oldest_child_and_date = (child, oldest_date)

            if oldest_child_and_date:
                main = oldest_child_and_date[0]
                children.remove(main)
            else:
                raise Exception('No main record found for a project %s' % id)

        self.__ppts_main = main
        self.__ppts_children = children

    @property
    def main(self):
        return self.__ppts_main

    @property
    def children(self):
        return self.__ppts_children

    def field(self, name):
        # for ppts, prefer parent record, only moving to children if none
        # found, at which point we choose the value with the latest
        # last_updated
        # TODO: I'm not even sure this is the correct logic to use for dealing
        # with ambiguities.
        val = ''
        if name in self.__ppts_main.values:
            val = self.__ppts_main.values[name]['value']

        update_date = datetime.min
        if val == '':
            for child in self.__ppts_children:
                if name in child.values:
                    if child.values[name]['last_updated'] > update_date:
                        update_date = child.values[name]['last_updated']
                        val = child.values[name]['value']

        return val
