class RivermillError(Exception):
    """Base error for job / input problems."""


class FormatError(RivermillError):
    pass


class NotImplementedEngine(RivermillError):
    pass
