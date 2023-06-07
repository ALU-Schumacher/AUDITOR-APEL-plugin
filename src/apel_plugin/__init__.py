from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("apel_plugin")
except PackageNotFoundError:
    # package is not installed
    pass
