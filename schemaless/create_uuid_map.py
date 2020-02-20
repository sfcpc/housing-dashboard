import argparse
from collections import defaultdict
from collections import OrderedDict
import csv
from csv import DictReader
from csv import DictWriter
import pandas as pd

import uuid

from datetime import date
from fileutils import open_file
from schemaless.mapblklot_generator import MapblklotGenerator
from schemaless.create_schemaless import latest_values
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import PPTS
from schemaless.sources import PTS
from schemaless.sources import source_map
from schemaless.sources import TCO


class RecordGraphBuilderHelper:
    SOURCE = None

    def __init__(self, graph_builder):
        self.graph_builder = graph_builder

    def preprocess(self, latest_records):
        """Set up the Helper.

        Use this to populate caches of ids->fks, among other setup.

        Args:
            latest_records: The output of
                schemaless.create_schemaless.latest_values
        """
        pass

    def process(self, fk, record, parents, children):
        """Process the given record to find parents and children.

        Args:
            fk: The PrimaryKey for this record.
            record: The record dict (from `latest_values`)
            parents: A list of fks. List is modified in place.
            children: A list of fks. List is modified in place.
        """
        pass

    def process_likely(self, fk, record, parents, children):
        """Process the given record to find *likely* parents and children.

        This is for handling records without explicit parent/child ID links.
        For example, matching by address, blocklot, or geometry.

        Args:
            fk: The PrimaryKey for this record.
            record: The record dict (from `latest_values`)
            parents: A list of fks. List is modified in place.
            children: A list of fks. List is modified in place.
        """
        pass


class PPTSAddressLookupMixin:
    """Mixin to look up PPTS records by normalized address."""
    def ppts_by_address(self, fk, record, parents, children):
        """Find PPTS parents for `record` that match its address.

        This will not add fks to parents or children if they already
        share a UUID.

        Args:
            fk: The PrimaryKey for this record.
            record: The record dict (from `latest_values`). Expects
                    `address_norm` to exist in the dict.
            parents: A list of fks. List is modified in place.
            children: A list of fks. List is modified in place.
        """
        # TODO: There will be many matches, so we need to filter
        # on time range and record type. EG maybe only add parents
        # that are PRJs, or themselves have parents?
        if 'address_norm' not in record:
            return
        ppts_helper = self.graph_builder.helpers[PPTS.NAME]
        parents.extend(ppts_helper.find_by_address(record['address_norm']))


class PTSAddressLookupMixin:
    """Mixin to look up PTS records by normalized address."""
    def pts_by_address(self, fk, record, parents, children):
        """Find PTS parents for `record` that match its address.

        Args:
            fk: The PrimaryKey for this record.
            record: The record dict (from `latest_values`). Expects
                    `address_norm` to exist in the dict.
            parents: A list of fks. List is modified in place.
            children: A list of fks. List is modified in place.
        """
        if 'address_norm' not in record:
            return
        pts_helper = self.graph_builder.helpers[PTS.NAME]
        parents.extend(
            pts_helper.find_by_address(record['address_norm']))


class CalculatedFieldsMixin:
    """Mixin to add all calculated fields for a source into the record."""
    def add_calculated_fields(self, record):
        calculated_fields = self.SOURCE.calculated_fields(record)
        record = record.copy()
        record.update(calculated_fields)
        return record


class PPTSHelper(RecordGraphBuilderHelper, CalculatedFieldsMixin):
    SOURCE = PPTS

    def __init__(self, graph_builder):
        super().__init__(graph_builder)
        self._ppts_id_to_fk = {}
        self._address_to_ppts_fk = defaultdict(list)
        self._permit_number_to_ppts_fk = defaultdict(list)

    def find_by_id(self, record_id):
        """Find a fk by PPTS record_id."""
        return self._ppts_id_to_fk.get(record_id, None)

    def find_by_address(self, address):
        """Find a fk by a normalized address."""
        return self._address_to_ppts_fk[address]

    def find_by_permit_number(self, permit_number):
        """Find a fk by PTS building permit number."""
        return self._permit_number_to_ppts_fk[permit_number]

    def preprocess(self, latest_records):
        for fk, record in latest_records.get(PPTS.NAME, {}).items():
            self._ppts_id_to_fk[record['record_id']] = fk
            if 'building_permit_number' in record:
                for permit_number in \
                        record['building_permit_number'].split(","):
                    self._permit_number_to_ppts_fk[permit_number].append(fk)
            record = self.add_calculated_fields(record)
            if 'address_norm' in record:
                self._address_to_ppts_fk[record['address_norm']].append(fk)

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


class PTSHelper(RecordGraphBuilderHelper,
                PPTSAddressLookupMixin,
                CalculatedFieldsMixin):
    SOURCE = PTS
    PERMIT_GROUPING_ATTRS = ['mapblklot', 'filed_date', 'proposed_use']

    def __init__(self, graph_builder):
        super().__init__(graph_builder)

        self._permit_number_to_pts_fk = defaultdict(list)
        self._address_to_pts_fk = defaultdict(list)

        # Maps "root" permits with their associated child permits.
        self._permit_groupings = defaultdict(list)

    def find_by_building_permit_number(self, permit_number):
        """Find a fk by PTS building permit number."""
        return self._permit_number_to_pts_fk[permit_number]

    def find_by_address(self, address):
        """Find a fk by a normalized address."""
        return self._address_to_pts_fk[address]

    def preprocess(self, latest_records):
        groupable_records = {}
        for fk, record in latest_records.get(PTS.NAME, {}).items():
            if 'permit_number' in record:
                self._permit_number_to_pts_fk[
                    record['permit_number']].append(fk)
            record = self.add_calculated_fields(record)
            if 'address_norm' in record:
                self._address_to_pts_fk[record['address_norm']].append(fk)

            if set(PTSHelper.PERMIT_GROUPING_ATTRS).issubset(record.keys()):
                groupable_records[fk] = record
        self.permit_groupings = self.compute_permit_groupings(groupable_records)
        print(self.permit_groupings)

    def compute_permit_groupings(self, groupable_records):
        groupable_records_df = pd.DataFrame(groupable_records.values())
        groupable_records_df['fk'] = groupable_records.keys()

        permit_groupings_df = groupable_records_df.groupby(PTSHelper.PERMIT_GROUPING_ATTRS)
        permit_groupings = {}
        for _, group in permit_groupings_df:
            group_fks = group['fk'].tolist()
            permit_groupings[group_fks[0]] = group_fks[1:]
        return permit_groupings

    def process(self, fk, record, parents, children):
        # Make sure that the permit is linked to other permits in its "group".
        if fk in self.permit_groupings.keys():
            children.extend(self.permit_groupings[fk])

        # Make sure that the permit is linked to other permits with the same
        # permit number.
        permit_number = record['permit_number']
        pts_helper = self.graph_builder.helpers[PTS.NAME]
        pts_records_with_permit_no = pts_helper.find_by_building_permit_number(permit_number)

        if pts_records_with_permit_no and pts_records_with_permit_no[0] != fk and pts_records_with_permit_no[0] not in children:
            parents.append(pts_records_with_permit_no[0])

        # Make sure that the permit is linked to any parent PPTS records.
        ppts_helper = self.graph_builder.helpers[PPTS.NAME]
        parent_ppts_records = ppts_helper.find_by_permit_number(permit_number)

        if parent_ppts_records:
            # If there is a PPTS record that should be the parent
            # record of PTS records with this permit number, we
            # assign that as the parent.
            parents.extend(parent_ppts_records)

    def process_likely(self, fk, record, parents, children):
        record = self.add_calculated_fields(record)
        self.ppts_by_address(fk, record, parents, children)


class TCOHelper(RecordGraphBuilderHelper,
                PPTSAddressLookupMixin,
                PTSAddressLookupMixin,
                CalculatedFieldsMixin):
    SOURCE = TCO

    def process(self, fk, record, parents, children):
        if 'building_permit_number' in record:
            pts_helper = self.graph_builder.helpers[PTS.NAME]
            parent_fk = pts_helper.find_by_building_permit_number(
                record['building_permit_number'])
            if parent_fk:
                parents.extend(parent_fk)

    def process_likely(self, fk, record, parents, children):
        record = self.add_calculated_fields(record)
        self.ppts_by_address(fk, record, parents, children)
        self.pts_by_address(fk, record, parents, children)


class MOHCDPipelineHelper(
        RecordGraphBuilderHelper,
        PPTSAddressLookupMixin,
        PTSAddressLookupMixin,
        CalculatedFieldsMixin):
    SOURCE = MOHCDPipeline

    def __init__(self, graph_builder):
        super().__init__(graph_builder)
        self._mohcd_id_to_fk = {}

    def find_by_id(self, project_id):
        """Find a fk by MOHCD project_id."""
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

    def process_likely(self, fk, record, parents, children):
        record = self.add_calculated_fields(record)
        self.ppts_by_address(fk, record, parents, children)
        self.pts_by_address(fk, record, parents, children)


class MOHCDInclusionaryHelper(
        RecordGraphBuilderHelper,
        PPTSAddressLookupMixin,
        PTSAddressLookupMixin,
        CalculatedFieldsMixin):
    SOURCE = MOHCDInclusionary

    def process(self, fk, record, parents, children):
        if 'planning_case_number' in record:
            ppts_helper = self.graph_builder.helpers[PPTS.NAME]
            for parent in record['planning_case_number'].split(","):
                parent_fk = ppts_helper.find_by_id(parent)
                if parent_fk:
                    parents.append(parent_fk)

        mohcd_pipeline_helper = self.graph_builder.helpers[MOHCDPipeline.NAME]
        parent_fk = mohcd_pipeline_helper.find_by_id(record['project_id'])
        if parent_fk:
            parents.append(parent_fk)

    def process_likely(self, fk, record, parents, children):
        record = self.add_calculated_fields(record)
        self.ppts_by_address(fk, record, parents, children)
        self.pts_by_address(fk, record, parents, children)


class AffordableRentalPortfolioHelper(
        RecordGraphBuilderHelper,
        PPTSAddressLookupMixin,
        PTSAddressLookupMixin,
        CalculatedFieldsMixin):
    SOURCE = AffordableRentalPortfolio

    def process_likely(self, fk, record, parents, children):
        record = self.add_calculated_fields(record)
        self.ppts_by_address(fk, record, parents, children)
        self.pts_by_address(fk, record, parents, children)


class PermitAddendaSummaryHelper(RecordGraphBuilderHelper):
    def process(self, fk, record, parents, children):
        pts_helper = self.graph_builder.helpers[PTS.NAME]
        parent_fks = pts_helper.find_by_building_permit_number(
            record['permit_number'])
        if parent_fks:
            parents.extend(parent_fks)


class RecordGraphBuilder:
    """RecordGraphBuilder reads in files and builds a RecordGraph."""

    def __init__(self, graph_class, schemaless_file, uuid_map_file, parcel_data_file,
                 find_likely_matches=False, exclude_known_likely_matches=True):
        """Init the graph builder.

        Args:
            graph_class: The class of graph to build (eg `RecordGraph`).
            schemaless_file: The path to a schemaless csv file.
            uuid_map_file: The path to a uuid mapping csv file.
        """
        self.graph_class = graph_class
        self.schemaless_file = schemaless_file
        self.uuid_map_file = uuid_map_file
        self.find_likely_matches = find_likely_matches
        self.exclude_known_likely_matches = exclude_known_likely_matches
        self.likelies = {}

        # Helpers expose an API that enables parent/child lookups by
        # various properties on records.
        self.helpers = {
            PPTS.NAME: PPTSHelper(self),
            PTS.NAME: PTSHelper(self),
            MOHCDPipeline.NAME: MOHCDPipelineHelper(self),
            MOHCDInclusionary.NAME: MOHCDInclusionaryHelper(self),
            TCO.NAME: TCOHelper(self),
            AffordableRentalPortfolio.NAME:
                AffordableRentalPortfolioHelper(self),
            PermitAddendaSummary.NAME:
                PermitAddendaSummaryHelper(self),
        }

    def build(self):
        """Build the graph."""
        rg = self.graph_class()

        latest_records = latest_values(self.schemaless_file)

        # preprocess every record with every helper to build the necessary
        # caches and maps.
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

        if not self.find_likely_matches:
            return rg

        # Now find likely matches
        for source_name, source_records in latest_records.items():
            for fk, record in source_records.items():
                likely_parents = []
                likely_children = []
                self.helpers[source_name].process_likely(
                    fk, record, likely_parents, likely_children)

                if self.exclude_known_likely_matches:
                    # Now that we have all likely parents, remove parents
                    # with an explicit or implied link
                    my_uuid = rg.get(fk).uuid
                    parents = []
                    children = []
                    for parent in likely_parents:
                        if rg.get(parent).uuid == my_uuid:
                            continue
                        parents.append(parent)
                else:
                    parents = likely_parents
                    children = likely_children
                self.likelies[fk] = {
                    'parents': parents,
                    'children': children,
                }

        return rg

    def write_likely_matches(self, outfile):
        with open(outfile, 'w') as lf:
            writer = DictWriter(
                lf, fieldnames=['record_id', 'parents', 'children'])
            writer.writeheader()
            for fk, rec in self.likelies.items():
                if rec['parents'] or rec['children']:
                    writer.writerow({
                        'record_id': fk,
                        'parents': ", ".join(rec['parents']),
                        'children': ", ".join(rec['children']),
                    })


class RecordGraph:
    """RecordGraph is a directed hierachy of records.

    It contains many disjoint DAGs that reproduce the hierarchy of records.
    Ideally, the root of each of those graphs is a PPTS PRJ record, but this
    is not strictly true. PTS (Building permits) records often lack a parent
    record in Planning's PPTS database, so the root there is just the first
    permit we can find.
    """
    def __init__(self):
        self._nodes = OrderedDict()

    @classmethod
    def from_files(cls,
                   schemaless_file,
                   uuid_map_file,
                   find_likely_matches=False,
                   exclude_known_likely_matches=True):
        return RecordGraphBuilder(
            cls,
            schemaless_file,
            uuid_map_file,
            find_likely_matches,
            exclude_known_likely_matches
        ).build()

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
        """Add a record to the graph.

        Args:
            record: A `Node` object to add to the graph.
        """
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
        """Link a parent and child together.

        Args:
            parent_record_id: The FK of the parent record.
            child_record_id: The FK of the child record.

        If either parent or child do not exist, they are created.
        """
        if parent_record_id not in self._nodes:
            self._nodes[parent_record_id] = Node(parent_record_id)
        if child_record_id not in self._nodes:
            self._nodes[child_record_id] = Node(child_record_id)
        self._nodes[parent_record_id].add_child(child_record_id)
        self._nodes[child_record_id].add_parent(parent_record_id)

    def get(self, record_id):
        """Get a Node by FK.

        Args:
            record_id: The FK to look up.
        """
        return self._nodes.get(record_id, None)

    def _resolve_parent(self, record_id):
        """Traverse the graph to finding the root parent."""
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
        """Assign a UUID to every Node in the graph.

        All children of a parent will be assigned the UUID of the parent.
        """
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
    parser.add_argument(
        '--likely_match_file',
        help='File to write likely parent/child matches to.',
        default='')
    parser.add_argument('outfile', help='Output path of uuid mapping')
    parser.add_argument('--parcel_data_file')

    args = parser.parse_args()

    if args.parcel_data_file:
        MapblklotGenerator(args.parcel_data_file)

    builder = RecordGraphBuilder(
        RecordGraph,
        args.schemaless_file,
        args.uuid_map_file,
        args.parcel_data_file,
        args.likely_match_file != "")
    rg = builder.build()

    rg.to_file(args.outfile)
    if args.likely_match_file:
        builder.write_likely_matches(args.likely_match_file)
