"""Pacote principal do analisador DETRAF."""

from . import import_detraf, match_cdr, normalizer, processing, main

__all__ = [
    "__version__",
    "import_detraf",
    "match_cdr",
    "normalizer",
    "processing",
    "main",
]

__version__ = "2.0.0"
