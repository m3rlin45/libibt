from __future__ import annotations
import os
from collections.abc import Callable
from typing import BinaryIO
from libibt.base import LogFile

def ibt(
    source: str | bytes | os.PathLike[str] | BinaryIO,
    progress: Callable[[int, int], None] | None = None,
) -> LogFile: ...
