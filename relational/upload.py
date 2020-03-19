# Lint as: python3
"""Upload all artifacts to datasf."""
import logging

import datasf
from relational import table


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


PROJECT_FACTS_VIEW_ID = "quwd-ukmk"
UNIT_COUNTS_FULL_VIEW_ID = "3npy-vcz5"
COMPLETED_UNIT_COUNTS_VIEW_ID = "i6zb-tv29"
PROJECT_STATUS_HISTORY_VIEW_ID = "52p2-4jnc"
PROJECT_GEO_VIEW_ID = "76ua-mi4f"
PROJECT_DETAILS_VIEW_ID = "ytmr-qrae"
DATA_FRESHNESS_VIEW_ID = "gfbi-9hdu"


TABLE_TO_VIEW_ID = {
    table.ProjectFacts: PROJECT_FACTS_VIEW_ID,
    table.ProjectUnitCountsFull: UNIT_COUNTS_FULL_VIEW_ID,
    table.ProjectCompletedUnitCounts: COMPLETED_UNIT_COUNTS_VIEW_ID,
    table.ProjectStatusHistory: PROJECT_STATUS_HISTORY_VIEW_ID,
    table.ProjectGeo: PROJECT_GEO_VIEW_ID,
    table.ProjectDetails: PROJECT_DETAILS_VIEW_ID,
}


def _replace(path, view_id):
    client = datasf.get_client()
    datasf.replace(client, view_id, path)


def upload_table(table, path):
    logger.info("Uploading %s...", table.__name__)
    _replace(path, TABLE_TO_VIEW_ID[table])


def upload_data_freshness(path):
    logger.info("Uploading data freshness...")
    _replace(path, DATA_FRESHNESS_VIEW_ID)
