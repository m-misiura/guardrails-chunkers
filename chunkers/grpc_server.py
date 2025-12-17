import logging
from concurrent import futures
from dataclasses import dataclass, field
from typing import Set, Tuple, List

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from . import caikit_data_model_nlp_pb2, chunkers_pb2_grpc, get_chunker_registry

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class StreamState:
    """Manages state for streaming chunker operations."""

    accumulated_text: str = ""
    processed_offset: int = 0
    input_index_tracker: List[int] = field(default_factory=list)
    chunk_count: int = 0
    yielded_chunks: Set[Tuple[int, int]] = field(default_factory=set)
    text_tracker: List[str] = field(default_factory=list)
    start_processing_counter: int = -1
    end_processing_counter: int = -1
    first_event: bool = True


class LoggingInterceptor(grpc.ServerInterceptor):
    """Interceptor to log all gRPC requests."""

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method
        logger.info(f"gRPC request received: {method}")

        try:
            response = continuation(handler_call_details)
            logger.info(f"gRPC request completed: {method}")
            return response
        except Exception as e:
            logger.error(f"gRPC request failed: {method}, error: {e}")
            raise


class ChunkersServicer(chunkers_pb2_grpc.ChunkersServiceServicer):
    """gRPC servicer for chunking operations."""

    def __init__(self):
        self.registry = get_chunker_registry()
        logger.info(f"Initialized chunker registry with: {self.registry.list_names()}")

    def ChunkerTokenizationTaskPredict(self, request, context):
        """Unary chunking request."""
        try:
            metadata = dict(context.invocation_metadata())
            model_id = metadata.get("mm-model-id", "sentence")

            logger.info(
                f"Received chunking request: model_id={model_id}, text_length={len(request.text)}"
            )

            chunker = self.registry.get(model_id)
            if not chunker:
                logger.error(
                    f"Unknown chunker: {model_id}. Available: {self.registry.list_names()}"
                )
                context.abort(grpc.StatusCode.NOT_FOUND, f"Unknown chunker: {model_id}")

            chunks = chunker.chunk(request.text)

            results = [
                caikit_data_model_nlp_pb2.Token(start=start, end=end, text=text)
                for text, start, end in chunks
            ]

            logger.info(
                f"Chunking complete: model_id={model_id}, chunks={len(results)}"
            )

            return caikit_data_model_nlp_pb2.TokenizationResults(
                results=results, token_count=len(results)
            )

        except Exception as e:
            logger.error(f"Chunking failed: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, str(e))

    def BidiStreamingChunkerTokenizationTaskPredict(self, request_iterator, context):
        """Streaming chunking request with text accumulation."""
        try:
            metadata = dict(context.invocation_metadata())
            model_id = metadata.get("mm-model-id", "sentence")

            logger.info(f"[STREAM] Starting bidirectional stream, model={model_id}")

            chunker = self.registry.get(model_id)
            if not chunker:
                logger.error(
                    f"Unknown chunker: {model_id}. Available: {self.registry.list_names()}"
                )
                context.abort(grpc.StatusCode.NOT_FOUND, f"Unknown chunker: {model_id}")

            state = StreamState()

            # Yield initial empty response to establish bidirectional stream
            yield self._create_stream_result([], 0, 0, 0, 0, 0)

            for request_count, request in enumerate(request_iterator, 1):
                # Accumulate text and track input index
                state.accumulated_text += request.text_stream
                state.text_tracker.append(request.text_stream)

                logger.info(
                    f"[STREAM] Msg #{request_count}: received {len(request.text_stream)} chars, "
                    f"idx={request.input_index_stream}, accumulated={len(state.accumulated_text)}"
                )

                if request.input_index_stream != -1:
                    state.input_index_tracker.append(request.input_index_stream)
                    state.end_processing_counter += 1
                    if state.start_processing_counter < 0:
                        state.start_processing_counter = 0

                # Run chunker on unprocessed text
                chunks = chunker.chunk(state.accumulated_text[state.processed_offset :])
                logger.info(
                    f"[STREAM] Chunker found {len(chunks)} sentences in unprocessed text"
                )

                # Yield complete chunks (buffer last one as it may be incomplete)
                if len(chunks) > 1:
                    logger.info(
                        f"[STREAM] Yielding {len(chunks)-1} complete sentences (buffering last)"
                    )
                    state.text_tracker = []

                    # Yield all but the last chunk
                    for text, start, end in chunks[:-1]:
                        yield from self._yield_chunk(state, text, start, end)

                    # Update processed offset after yielding all chunks
                    _, _, last_end = chunks[-2]
                    state.processed_offset += last_end
                    state.start_processing_counter = state.end_processing_counter

            # Stream complete - yield remaining chunks
            yield from self._yield_remaining_chunks(chunker, state)

        except Exception as e:
            logger.error(f"Stream chunking failed: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, str(e))

    def _yield_chunk(self, state: StreamState, text: str, start: int, end: int):
        """Helper to yield a single chunk with proper position tracking."""
        abs_start = start + state.processed_offset
        abs_end = end + state.processed_offset

        # Skip if already yielded
        if (abs_start, abs_end) in state.yielded_chunks:
            return

        # Handle leading whitespace for first event
        if state.first_event and abs_start != 0:
            abs_start = 0
            text = state.accumulated_text[abs_start:abs_end]

        state.yielded_chunks.add((abs_start, abs_end))
        state.chunk_count += 1

        # Mark first event as complete after first yield
        if state.first_event:
            state.first_event = False

        # Calculate input indices
        chunk_input_start, chunk_input_end = self._calculate_input_indices(state)

        yield self._create_stream_result(
            [caikit_data_model_nlp_pb2.Token(start=abs_start, end=abs_end, text=text)],
            chunk_input_start,
            chunk_input_end,
            abs_start,
            abs_end,
            1,
        )

    def _yield_remaining_chunks(self, chunker, state: StreamState):
        """Yield any remaining chunks after stream completes."""
        remaining_chunks = chunker.chunk(
            state.accumulated_text[state.processed_offset :]
        )

        if remaining_chunks:
            for text, start, end in remaining_chunks:
                yield from self._yield_chunk(state, text, start, end)
        else:
            # Handle whitespace-only or no-chunk case
            yield from self._yield_text_tracker(state)

    def _yield_text_tracker(self, state: StreamState):
        """Yield text chunks as-is when no sentences are detected."""
        sentence_start = 0
        chunk_input_start, chunk_input_end = self._calculate_input_indices(state)

        for text_chunk in state.text_tracker:
            sentence_end = sentence_start + len(text_chunk)
            state.chunk_count += 1

            yield self._create_stream_result(
                [
                    caikit_data_model_nlp_pb2.Token(
                        start=sentence_start, end=sentence_end, text=text_chunk
                    )
                ],
                chunk_input_start,
                chunk_input_end,
                sentence_start,
                sentence_end,
                1,
            )
            sentence_start = sentence_end

    def _calculate_input_indices(self, state: StreamState) -> Tuple[int, int]:
        """Calculate input index range for current chunk."""
        start_counter = state.start_processing_counter
        end_counter = state.end_processing_counter

        # Clamp start counter to valid range
        if start_counter >= len(state.input_index_tracker):
            start_counter = len(state.input_index_tracker) - 1

        chunk_input_start = (
            state.input_index_tracker[start_counter] if start_counter >= 0 else 0
        )
        chunk_input_end = (
            state.input_index_tracker[end_counter - 1] if end_counter > 0 else 0
        )

        return chunk_input_start, chunk_input_end

    @staticmethod
    def _create_stream_result(results, input_start, input_end, start, processed, count):
        """Create a ChunkerTokenizationStreamResult."""
        return caikit_data_model_nlp_pb2.ChunkerTokenizationStreamResult(
            results=results,
            input_start_index=input_start,
            input_end_index=input_end,
            start_index=start,
            processed_index=processed,
            token_count=count,
        )


def serve():
    """Start the gRPC server."""
    interceptors = [LoggingInterceptor()]

    options = [
        # Keepalive
        ("grpc.http2.min_ping_interval_without_data_ms", 10000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.keepalive_timeout_ms", 60000),
        # Resource limits
        ("grpc.http2.max_concurrent_streams", 500),
        ("grpc.max_receive_message_length", 10 * 1024 * 1024),
        ("grpc.max_send_message_length", 10 * 1024 * 1024),
        # Connection lifecycle
        ("grpc.max_connection_age_ms", 30 * 60 * 1000),
        ("grpc.max_connection_idle_ms", 10 * 60 * 1000),
    ]

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=200),
        interceptors=interceptors,
        options=options,
    )

    chunkers_pb2_grpc.add_ChunkersServiceServicer_to_server(ChunkersServicer(), server)

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    # Enable gRPC reflection
    SERVICE_NAMES = (
        "caikit.runtime.Chunkers.ChunkersService",
        "grpc.health.v1.Health",
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port("[::]:8085")

    registry = get_chunker_registry()
    available_chunkers = ", ".join(registry.list_names())

    logger.info("=" * 80)
    logger.info("gRPC server listening on port 8085")
    logger.info(f"Available chunkers: {available_chunkers}")
    logger.info("Health check endpoint: grpc.health.v1.Health/Check")
    logger.info("=" * 80)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
