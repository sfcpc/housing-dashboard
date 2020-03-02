from csv import DictReader
import re
import threading

from fileutils import open_file


def init(filepath):
    MapblklotGeneratorSingleton(filepath)


class MapblklotGeneratorSingleton:
    _instance = None

    def _extract_lnglat(self, mp):
        '''Extracts an arbitrary longitude and latitude from the data source.

        This originally found a centroid, but this was less error-prone and
        faster.

        Returns:
            A tuple of (longitude, latitude) as strings'''
        parts = re.match(r'MULTIPOLYGON \(\(\(([-.0-9]+)\s+([-.0-9]+)', mp)

        if len(parts.groups()) != 2:
            raise RuntimeError('Invalid map coord: %s' % mp)

        return parts.group(1), parts.group(2)

    def __init__(self, filepath):
        '''Sets up the generator based on mapblklot source data.
            Args:
                filepath: Filepath to the dataset that contains blklot to mapblklot mappings.
                    (Currently https://data.sfgov.org/Geographic-Locations-and-Boundaries/Parcels-Active-and-Retired/acdm-wktn/data) # NOQA
        '''
        with threading.Lock():
            if MapblklotGeneratorSingleton._instance:
                raise RuntimeError("Already instantiated")

            self._blklot_to_mapblklot = {}
            self._blklot_to_latlng = {}
            with open_file(filepath, mode='rt') as inf:
                reader = DictReader(inf)
                for line in reader:
                    self._blklot_to_mapblklot[line['blklot']] = line[
                        'mapblklot']
                    self._blklot_to_latlng[line['blklot']] = \
                        self._extract_lnglat(line['shape'])

            MapblklotGeneratorSingleton._instance = self

    def find_mapblklot_for_blklot(self, blklot):
        """Returns the mapblklot for the given blklot"""
        if blklot in self._blklot_to_mapblklot:
            return self._blklot_to_mapblklot[blklot]
        return None

    def find_lnglat_for_blklot(self, blklot):
        '''Returns a tuple of (long, lat) for a blklot, None if not found'''
        return self._blklot_to_latlng.get(blklot, None)

    @classmethod
    def get_instance(cls):
        return cls._instance
