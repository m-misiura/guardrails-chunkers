import re
from typing import List, Tuple, Optional

from .base_chunker import BaseChunker


class SentenceChunker(BaseChunker):
    """Chunk text into sentences using regex pattern."""

    DEFAULT_PATTERN = r"[.!?]+(?=\s|\n|$)|\n"

    def __init__(self):
        """Initialize with pre-compiled regex for performance."""
        self._compiled_regex = re.compile(self.DEFAULT_PATTERN)

    @property
    def name(self) -> str:
        return "sentence"

    def chunk(
        self, text: str, pattern: str = None, **kwargs
    ) -> List[Tuple[str, int, int]]:
        """
        Split text into sentences.

        Args:
            text: Input text to split
            pattern: Optional custom regex pattern
            **kwargs: Additional parameters (ignored)

        Returns:
            List of (sentence, start_pos, end_pos) tuples
        """
        if not text.strip():
            return []

        regex = re.compile(pattern) if pattern else self._compiled_regex
        sentences = []
        last_end = 0

        # Process matched sentences
        for match in regex.finditer(text):
            sentence = self._extract_sentence(text, last_end, match.end())
            if sentence:
                sentences.append(sentence)
            last_end = match.end()

        # Process remaining text after last match
        if last_end < len(text):
            sentence = self._extract_sentence(text, last_end, len(text))
            if sentence:
                sentences.append(sentence)

        return sentences if sentences else [(text.strip(), 0, len(text.strip()))]

    @staticmethod
    def _extract_sentence(
        text: str, start: int, end: int
    ) -> Optional[Tuple[str, int, int]]:
        """
        Extract and calculate positions for a sentence.

        Args:
            text: Full text
            start: Start position in text
            end: End position in text

        Returns:
            (stripped_text, start_pos, end_pos) tuple or None if empty
        """
        raw_sentence = text[start:end]
        stripped = raw_sentence.strip()

        if not stripped:
            return None

        leading_spaces = len(raw_sentence) - len(raw_sentence.lstrip())
        start_pos = start + leading_spaces
        end_pos = start_pos + len(stripped)

        return (stripped, start_pos, end_pos)
