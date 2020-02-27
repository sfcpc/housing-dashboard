from csv import DictReader
import threading

from fileutils import open_file


def init(filepath):
    MapblklotGeneratorSingleton(filepath)


class MapblklotGeneratorSingleton:
    _instance = None

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
            with open_file(filepath, mode='rt') as inf:
                reader = DictReader(inf)
                for line in reader:
                    self._blklot_to_mapblklot[line['blklot']] = line[
                        'mapblklot']
            MapblklotGeneratorSingleton._instance = self

    def find_mapblklot_for_blklot(self, blklot):
        """Returns the mapblklot for the given blklot"""
        if blklot in self._blklot_to_mapblklot:
            return self._blklot_to_mapblklot[blklot]
        return None

    @classmethod
    def get_instance(cls):
        return cls._instance
