"""
Chunker microservice for FMS Guardrails Orchestrator.
"""

from .base_chunker import BaseChunker
from .chunker_registry import ChunkerRegistry
from .sentence_chunker import SentenceChunker

# Create and populate the global registry
_registry = ChunkerRegistry()
_registry.register(SentenceChunker())


def get_chunker_registry() -> ChunkerRegistry:
    """Get the global chunker registry."""
    return _registry


__all__ = [
    "BaseChunker",
    "SentenceChunker",
    "ChunkerRegistry",
    "get_chunker_registry",
]
