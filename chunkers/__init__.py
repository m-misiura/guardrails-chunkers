"""
Chunker microservice for FMS Guardrails Orchestrator.
"""

import logging

from .base_chunker import BaseChunker
from .chunker_factory import ChunkerFactory
from .chunker_registry import ChunkerRegistry
from .sentence_chunker import SentenceChunker

logger = logging.getLogger(__name__)

# Create and populate the global registry
_registry = ChunkerRegistry()
_registry.register(SentenceChunker())
try:
    factory_chunkers = ChunkerFactory.create_from_config()
    for chunker in factory_chunkers:
        _registry.register(chunker)
    logger.info(f"Registered {len(factory_chunkers)} chunkers from configuration")
except Exception as e:
    logger.warning(f"Failed to load chunkers from configuration: {e}")


def get_chunker_registry() -> ChunkerRegistry:
    """Get the global chunker registry."""
    return _registry


__all__ = [
    "BaseChunker",
    "SentenceChunker",
    "ChunkerRegistry",
    "ChunkerFactory",
    "get_chunker_registry",
]
