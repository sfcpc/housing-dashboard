# Lint as: python3
from csv import DictReader
from fileutils import open_file

# MapblklotGenerator instance that can be used by any python module that
# imports it to find mapblklots. This should be instantiated in the top-level
# script environment.
generator_instance = None


class MapblklotGenerator:
    def __init__(self, filepath):
        '''Sets up the generator based on mapblklot source data.

          Args:
              filepath: Filepath to the dataset that contains blklot to mapblklot mappings.
                    (Currently https://data.sfgov.org/Geographic-Locations-and-Boundaries/Parcels-Active-and-Retired/acdm-wktn/data) # NOQA
            '''
        blklot_to_mapblklot = {}
        with open_file(filepath, mode='rt') as inf:
            reader = DictReader(inf)
            for line in reader:
                blklot_to_mapblklot[line['blklot']] = line['mapblklot']
        self._blklot_to_mapblklot = blklot_to_mapblklot

    def find_mapblklot_for_blklot(self, blklot):
        """Returns the mapblklot for the given blklot"""
        if blklot in self._blklot_to_mapblklot:
            return self._blklot_to_mapblklot[blklot]
        return None
