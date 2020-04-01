# Lint as: python3
"""Utils for working with files."""
import lzma
import pathlib


def open_file(fname, *args, **kwargs):
    if pathlib.Path(fname).suffix.endswith('.xz'):
        o = lzma.open
    else:
        o = open
    return o(fname, *args, **kwargs)
