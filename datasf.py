# Lint as: python3
"""Tools to upload files to DataSF."""
import logging
from pathlib import Path

from socrata.authorization import Authorization
from socrata import Socrata

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_client(user, password):
    """Get a Socrata client for the given username and password."""
    auth = Authorization("data.sfgov.org", user, password)
    return Socrata(auth)


def upsert(client, view_id, fp, public=False):
    """Upsert a file to a DataSF dataset.

    Args:
        client: A Socrata client (from `get_client`)
        view_id: The "view id" of the Socrata dataset. It is the
            "5udx-x8qv" part of data.sfgov.org/dataset/foo/5udx-x8qv.
        fp: Path for a file to upsert.
        public: Bool. Is the dataset public?

    Note that upsert will *append* the given file to the end of the dataset,
    unless the file contains row_ids that already exist in the dataset, in
    which case those rows will be updated.
    """
    view = client.views.lookup(view_id)
    permission = 'public' if public else 'private'
    rev = view.revisions.create_update_revision(permission=permission)
    pp = Path(fp)
    upload = rev.create_upload(pp.name)
    logger.info("Uploading %s", pp.name)
    with open(fp, 'rb') as inf:
        upload.csv(inf)
    job = rev.apply()
    job.wait_for_finish(progress=lambda job: print(job.attributes['log']))


def replace(client, view_id, fp, public=False):
    """Replace a dataset with the contents of a file.

    Args:
        client: A Socrata client (from `get_client`)
        view_id: The "view id" of the Socrata dataset. It is the
            "5udx-x8qv" part of data.sfgov.org/dataset/foo/5udx-x8qv.
        fp: Path for a file to upload.
        public: Bool. Is the dataset public?
    """
    view = client.views.lookup(view_id)
    permission = 'public' if public else 'private'
    rev = view.revisions.create_replace_revision(permission=permission)
    pp = Path(fp)
    upload = rev.create_upload(pp.name)
    logger.info("Uploading %s", pp.name)
    with open(fp, 'rb') as inf:
        upload.csv(inf)
    job = rev.apply()
    job.wait_for_finish(progress=lambda job: print(job.attributes['log']))
