# Lint as: python3
"""Utils for working with files."""
import lzma


def open_file(fname, *args, **kwargs):
    if fname.endswith('.xz'):
        o = lzma.open
    else:
        o = open
    return o(fname, *args, **kwargs)
