# Lint as: python3
from csv import DictReader
from fileutils import open_file


class MapblklotGenerator():
    class __MapblklotGeneratorImpl:
        def __init__(self, filepath):
            blklot_to_mapblklot = {}
            with open_file(filepath, mode='rt') as inf:
                reader = DictReader(inf)
                for line in reader:
                    blklot_to_mapblklot[line['blklot']] = line['mapblklot']
            self._blklot_to_mapblklot = blklot_to_mapblklot

        def find_mapblklot_for_blklot(self, blklot):
            if blklot in self._blklot_to_mapblklot:
                return self._blklot_to_mapblklot[blklot]
            return None

    __instance = None

    def __init__(self, filepath):
        if not MapblklotGenerator.__instance:
            MapblklotGenerator.__instance = \
                    MapblklotGenerator.__MapblklotGeneratorImpl(filepath)

    @staticmethod
    def get_instance():
        return MapblklotGenerator.__instance
