import argparse
from collections import defaultdict
from collections import OrderedDict
import csv
from csv import DictReader
import uuid

from datetime import date
from fileutils import open_file
from schemaless.create_schemaless import latest_values
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import PPTS
from schemaless.sources import PTS
from schemaless.sources import source_map
from schemaless.sources import TCO


class RecordGraphBuilderHelper:
    def __init__(self, graph_builder):
        self.graph_builder = graph_builder

    def preprocess(self, latest_records):
        pass

    def process(self, fk, record, parents, children):
        pass


class PPTSHelper(RecordGraphBuilderHelper):
    def __init__(self, graph_builder):
        super().__init__(graph_builder)
        self._ppts_id_to_fk = {}
        self._address_to_ppts_fk = defaultdict(list)
        self._permit_number_to_ppts_fk = defaultdict(list)

    def find_by_id(self, record_id):
        return self._ppts_id_to_fk.get(record_id, None)

    def find_by_address(self, address):
        return self._address_to_ppts_fk[address]

    def find_by_permit_number(self, permit_number):
        return self._permit_number_to_ppts_fk[permit_number]

    def preprocess(self, latest_records):
        for fk, record in latest_records.get(PPTS.NAME, {}).items():
            self._ppts_id_to_fk[record['record_id']] = fk
            if 'address_norm' in record:
                self._address_to_ppts_fk[record['address_norm']].append(fk)
            if 'building_permit_number' in record:
                for permit_number in \
                        record['building_permit_number'].split(","):
                    self._permit_number_to_ppts_fk[permit_number].append(fk)

    def process(self, fk, record, parents, children):
        if 'parent' in record:
            for parent in record['parent'].split(","):
                parent_fk = self.find_by_id(parent)
                if parent_fk:
                    parents.append(parent_fk)
        if 'children' in record:
            for child in record['children'].split(","):
                child_fk = self.find_by_id(child)
                if child_fk:
                    children.append(child_fk)


class PTSHelper(RecordGraphBuilderHelper):
    def __init__(self, graph_builder):
        super().__init__(graph_builder)
        self._permit_number_to_pts_fk = defaultdict(list)
        self._address_to_pts_fk = defaultdict(list)

    def find_by_building_permit_number(self, permit_number):
        return self._permit_number_to_pts_fk[permit_number]

    def find_by_address(self, address):
        return self._address_to_pts_fk[address]

    def preprocess(self, latest_records):
        for fk, record in latest_records.get(PTS.NAME, {}).items():
            if 'address_norm' in record:
                self._address_to_pts_fk[record['address_norm']].append(fk)
            if 'permit_number' in record:
                self._permit_number_to_pts_fk[
                    record['permit_number']].append(fk)

    def process(self, fk, record, parents, children):
        permit_number = record['permit_number']

        ppts_helper = self.graph_builder.helpers[PPTS.NAME]
        ppts_records = ppts_helper.find_by_permit_number(permit_number)

        pts_helper = self.graph_builder.helpers[PTS.NAME]
        pts_records = pts_helper.find_by_building_permit_number(permit_number)
        if ppts_records:
            # If there is a PPTS record that should be the parent
            # record of PTS records with this permit number, we
            # assign that as the parent.
            parents.extend(ppts_records)
        elif pts_records and pts_records[0] == fk:
            # Even if there is no PPTS record that should be the
            # parent of PTS records with this permit number, we need
            # to ensure that all PTS records with the same permit number
            # are assigned the same UUID.
            #
            # We do this by picking the first PTS record with the given
            # permit number and assigning any other records with
            # that permit number  as "children" of that record.
            children.extend(pts_records[1:])


class TCOHelper(RecordGraphBuilderHelper):
    def process(self, fk, record, parents, children):
        if 'building_permit_number' in record:
            pts_helper = self.graph_builder.helpers[PTS.NAME]
            parent_fk = pts_helper.find_by_building_permit_number(
                record['building_permit_number'])
            if parent_fk:
                parents.extend(parent_fk)


class MOHCDPipelineHelper(RecordGraphBuilderHelper):
    def __init__(self, graph_builder):
        super().__init__(graph_builder)
        self._mohcd_id_to_fk = {}

    def find_by_id(self, project_id):
        return self._mohcd_id_to_fk.get(project_id, None)

    def preprocess(self, latest_records):
        for fk, record in latest_records.get(MOHCDPipeline.NAME, {}).items():
            self._mohcd_id_to_fk[record['project_id']] = fk

    def process(self, fk, record, parents, children):
        if 'planning_case_number' in record:
            ppts_helper = self.graph_builder.helpers[PPTS.NAME]
            for parent in record['planning_case_number'].split(","):
                parent_fk = ppts_helper.find_by_id(parent)
                if parent_fk:
                    parents.append(parent_fk)


class MOHCDInclusionaryHelper(RecordGraphBuilderHelper):
    def __init__(self, graph_builder):
        super().__init__(graph_builder)
        self._mohcd_id_to_fk = {}

    def find_by_id(self, project_id):
        return self._mohcd_id_to_fk.get(project_id, None)

    def preprocess(self, latest_records):
        for fk, record in latest_records.get(MOHCDPipeline.NAME, {}).items():
            self._mohcd_id_to_fk[record['project_id']] = fk

    def process(self, fk, record, parents, children):
        mohcd_pipeline_helper = self.graph_builder.helpers[
            MOHCDPipeline.NAME]
        mohcd_pipeline_helper.process(fk, record, parents, children)
        parent_fk = mohcd_pipeline_helper.find_by_id(record['project_id'])
        if parent_fk:
            parents.append(parent_fk)
        parent_fk = self.find_by_id(record['project_id'])
        if parent_fk:
            parents.append(parent_fk)


class AffordableRentalPortfolioHelper(RecordGraphBuilderHelper):
    def process(self, fk, record, parents, children):
        # TODO: There will be many matches, so we need to filter
        # on time range and record type. EG maybe only add parents
        # that are PRJs, or themselves have parents?
        if 'address_norm' in record:
            pts_helper = self.graph_builder.helpers[PTS.NAME]
            parents.extend(
                pts_helper.find_by_address(record['address_norm']))
            ppts_helper = self.graph_builder.helpers[PPTS.NAME]
            parents.extend(
                ppts_helper.find_by_address(record['address_norm']))


class RecordGraphBuilder:
    def __init__(self, graph_class, schemaless_file, uuid_map_file):
        self.graph_class = graph_class
        self.schemaless_file = schemaless_file
        self.uuid_map_file = uuid_map_file

        self.helpers = {
            PPTS.NAME: PPTSHelper(self),
            PTS.NAME: PTSHelper(self),
            MOHCDPipeline.NAME: MOHCDPipelineHelper(self),
            MOHCDInclusionary.NAME: MOHCDInclusionaryHelper(self),
            TCO.NAME: TCOHelper(self),
            AffordableRentalPortfolio.NAME:
                AffordableRentalPortfolioHelper(self),
        }

    def build(self):
        """Build a RecordGraph from schemaless & uuid_map files."""
        rg = self.graph_class()

        latest_records = latest_values(self.schemaless_file)

        # TODO: Refactor this "pre-processing" logic?
        # Create a mapping between permit numbers and their associated PPTS
        # record (so we an ensure they are assigned the same UUID).
        for helper in self.helpers.values():
            helper.preprocess(latest_records)

        # Read the latest values from the schemaless file to build the graph.
        for source_name, source_records in latest_records.items():
            for fk, record in source_records.items():
                parents = []
                children = []
                self.helpers[source_name].process(
                    fk, record, parents, children)

                the_date = None

                if source_map[source_name].DATE.field in record:
                    the_date = source_map[source_name].DATE.get_value(record)
                rg.add(Node(
                    record_id=fk,
                    date=the_date if the_date else date.min,
                    parents=parents,
                    children=children,
                    uuid=None,
                ))

        # Read existing record_id->uuid mapping from the existing schemaless
        # map and update nodes with exisitng UUIDs.
        if self.uuid_map_file != '':
            with open_file(self.uuid_map_file, 'r') as f:
                reader = DictReader(f)
                for line in reader:
                    fk = line['fk']
                    if fk in rg:
                        rg._nodes[fk].uuid = line['uuid']
                    else:
                        print("Error: unknown id %s" % fk)

        # Resolve all parent UUIDs and generate new UUIDs
        rg._assign_uuids()
        return rg


class RecordGraph:
    def __init__(self):
        self._nodes = OrderedDict()

    @classmethod
    def from_files(cls, schemaless_file, uuid_map_file):
        return RecordGraphBuilder(cls, schemaless_file, uuid_map_file).build()

    def to_file(self, outfile):
        """Dump the uuid->fk map."""
        with open_file(outfile, 'w') as f:
            # We use an OrderedDict for self._nodes, so ensure a consistent
            # ordering across runs.
            writer = csv.writer(f)
            writer.writerow(['uuid', 'fk'])
            for fk, record in self._nodes.items():
                writer.writerow([record.uuid, fk])

    def add(self, record):
        rid = record.record_id
        if rid in self._nodes:
            # This may either be an update or adding a parent/child for an
            # existing node.
            node = self._nodes[rid]
            node.date = record.date
            if not node.uuid and record.uuid:
                node.uuid = record.uuid
            node.parents.update(record.parents)
            node.children.update(record.children)
            record = node
        else:
            self._nodes[rid] = record

        for parent in record.parents:
            self.link(parent, record.record_id)
        for child in record.children:
            self.link(record.record_id, child)

    def link(self, parent_record_id, child_record_id):
        if parent_record_id not in self._nodes:
            self._nodes[parent_record_id] = Node(parent_record_id)
        if child_record_id not in self._nodes:
            self._nodes[child_record_id] = Node(child_record_id)
        self._nodes[parent_record_id].add_child(child_record_id)
        self._nodes[child_record_id].add_parent(parent_record_id)

    def get(self, record_id):
        return self._nodes.get(record_id, None)

    def _resolve_parent(self, record_id):
        record = self.get(record_id)
        # Note: by not short-circuiting when a record already has a UUID, we
        # ensure that children who are reassigned to new parents always get the
        # correct parent id.
        if not record.parents:
            return record
        all_parents = []
        for idx, pid in enumerate(record.parents):
            if pid not in self._nodes or self._nodes[pid].date is None:
                # This implies this record is bad data and cannot be properly
                # connected to a real parent record.
                continue
            all_parents.append(self._resolve_parent(pid))
        # If all_parents is empty, then none of the parents in
        # record['parents'] actually exist as valid records. So we can't link
        # this to an exisitng uuid, so just return itself.
        # # TODO: Come back to this when we are reading in DBI. At least a few
        # of these records have related building permits.
        if not all_parents:
            return record
        # Sort on the tuple of (date, record_id) so we have a stable
        # ordering for the same dates.
        return sorted(all_parents,
                      key=lambda x: (x.date, x.record_id),
                      reverse=True)[0]

    def _assign_uuids(self):
        seen = set()
        for fk, record in self._nodes.items():
            if fk in seen:
                continue
            # 'parent' is a bit of a misnomer -- it may be itself!
            parent = self._resolve_parent(fk)
            puid = parent.uuid
            if not puid:
                if record.uuid:
                    puid = record.uuid
                else:
                    puid = uuid.uuid4()
                self.get(parent.record_id).uuid = puid
            self.get(fk).uuid = puid
            seen.add(fk)

    def __contains__(self, obj):
        return obj in self._nodes

    def __len__(self):
        return len(self._nodes)

    def items(self):
        return self._nodes.items()

    def keys(self):
        return self._nodes.keys()

    def values(self):
        return self._nodes.values()


class Node:
    def __init__(self,
                 record_id,
                 date=None,
                 parents=None,
                 children=None,
                 uuid=None):
        self.record_id = record_id
        self.date = date
        if parents is None:
            self.parents = set()
        else:
            self.parents = set(parents)
        if children is None:
            self.children = set()
        else:
            self.children = set(children)
        self.uuid = uuid

    def add_child(self, child_record_id):
        self.children.add(child_record_id)

    def add_parent(self, parent_record_id):
        self.parents.add(parent_record_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('schemaless_file', help='Schemaless csv file')
    parser.add_argument(
            '--uuid_map_file',
            help='UUID mapping file',
            default='')
    parser.add_argument('outfile', help='Output path of uuid mapping')
    args = parser.parse_args()

    rg = RecordGraph.from_files(args.schemaless_file, args.uuid_map_file)
    rg.to_file(args.outfile)
