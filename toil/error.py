__all__ = ['ToilError', 'InvalidURIError']


class ToilError(Exception):
    """Toil exception base class.
    """


class InvalidURIError(ToilError):
    """Invalid backend URI.
    """
