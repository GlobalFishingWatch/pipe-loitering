import importlib.metadata


def get_pipe_ver():
    """Returns the version of the package."""

    return importlib.metadata.version("pipe-loitering")
