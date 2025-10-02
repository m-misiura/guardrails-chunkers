import importlib
import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml

from .base_chunker import BaseChunker

logger = logging.getLogger(__name__)


class LangChainChunker(BaseChunker):
    """Wrapper for LangChain text splitters."""

    def __init__(self, name: str, class_path: str, **config):
        self._name = name
        self._splitter = self._create_splitter(class_path, config)

    @property
    def name(self) -> str:
        return f"langchain_{self._name}"

    def _create_splitter(self, class_path: str, config: Dict[str, Any]):
        """Create the text splitter instance."""
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        splitter_class = getattr(module, class_name)
        return splitter_class(**config)

    def chunk(self, text: str, **kwargs) -> List[tuple[str, int, int]]:
        """Split text into chunks."""
        if not text.strip():
            return []

        chunks = self._splitter.split_text(text)
        return self._calculate_positions(text, chunks)

    def _calculate_positions(
        self, text: str, chunks: List[str]
    ) -> List[tuple[str, int, int]]:
        """Calculate start/end positions for chunks."""
        result = []
        search_start = 0

        for chunk in chunks:
            if not chunk.strip():
                continue

            start_pos = text.find(chunk, search_start)
            if start_pos == -1:
                start_pos = search_start

            end_pos = start_pos + len(chunk)
            result.append((chunk, start_pos, end_pos))
            search_start = start_pos + 1

        return result


class ChunkerFactory:
    """Factory for creating LangChain chunkers."""

    @classmethod
    def create_from_config(
        cls, config_path: str = "chunker_config.yaml"
    ) -> List[BaseChunker]:
        """Create chunkers from YAML config file."""
        chunkers = []
        config_path = Path(__file__).parent / config_path

        if not config_path.exists():
            logger.warning(f"Config file not found: {config_path}")
            return chunkers

        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)

            for name, chunker_config in config.items():
                try:
                    chunker = LangChainChunker(
                        name=name,
                        class_path=chunker_config["class"],
                        **chunker_config.get("defaults", {}),
                    )
                    chunkers.append(chunker)
                    logger.info(f"Created chunker: {chunker.name}")

                except Exception as e:
                    logger.warning(f"Skipping {name}: {e}")

        except Exception as e:
            logger.error(f"Failed to load config: {e}")

        return chunkers
