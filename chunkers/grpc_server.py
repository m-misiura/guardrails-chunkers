import logging
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from . import (caikit_data_model_nlp_pb2, chunkers_pb2_grpc,
               get_chunker_registry)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
            logger.debug(
                f"Chunk details: {[(r.start, r.end, r.text[:50]) for r in results]}"
            )

            response = caikit_data_model_nlp_pb2.TokenizationResults(
                results=results, token_count=len(results)
            )

            logger.info(f"Sending response: token_count={response.token_count}")
            return response

        except Exception as e:
            logger.error(f"Chunking failed: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, str(e))

    def BidiStreamingChunkerTokenizationTaskPredict(self, request_iterator, context):
        """Streaming chunking request with text accumulation."""
        try:
            metadata = dict(context.invocation_metadata())
            model_id = metadata.get("mm-model-id", "sentence")

            chunker = self.registry.get(model_id)
            if not chunker:
                logger.error(
                    f"Unknown chunker: {model_id}. Available: {self.registry.list_names()}"
                )
                context.abort(grpc.StatusCode.NOT_FOUND, f"Unknown chunker: {model_id}")

            # Accumulate text and track input indices
            accumulated_text = ""
            processed_offset = 0
            input_index_tracker = []
            chunk_count = 0
            yielded_chunks = set()  # Track (start, end) to avoid duplicates
            text_tracker = []  # Track text chunks for whitespace-only case
            start_processing_counter = -1
            end_processing_counter = -1
            first_event = True

            request_count = 0

            # Yield initial empty response to establish bidirectional stream
            # This prevents deadlock where client waits for first response before sending data
            yield caikit_data_model_nlp_pb2.ChunkerTokenizationStreamResult(
                results=[],
                input_start_index=0,
                input_end_index=0,
                start_index=0,
                processed_index=0,
                token_count=0,
            )

            for request in request_iterator:
                request_count += 1

                # Accumulate text and track input index
                accumulated_text += request.text_stream
                text_tracker.append(request.text_stream)

                if request.input_index_stream != -1:
                    input_index_tracker.append(request.input_index_stream)
                    end_processing_counter += 1
                    if start_processing_counter < 0:
                        start_processing_counter = 0

                # Run chunker on the portion of text not yet processed
                chunks = chunker.chunk(accumulated_text[processed_offset:])

                # Only yield complete chunks (keep last one buffered)
                if len(chunks) > 1:
                    # Clear text tracker when sentences are detected
                    text_tracker = []

                    # Yield all but the last chunk
                    for idx, (text, start, end) in enumerate(chunks[:-1]):
                        # Adjust positions to be absolute (add offset)
                        abs_start = start + processed_offset
                        abs_end = end + processed_offset

                        # Skip if already yielded
                        if (abs_start, abs_end) in yielded_chunks:
                            continue

                        # Handle leading whitespace for first event
                        if first_event and abs_start != 0:
                            abs_start = 0
                            text = accumulated_text[abs_start:abs_end]
                            first_event = False

                        yielded_chunks.add((abs_start, abs_end))
                        chunk_count += 1

                        # Calculate input index range for this chunk
                        if start_processing_counter >= len(input_index_tracker):
                            start_processing_counter = len(input_index_tracker) - 1

                        chunk_input_start = input_index_tracker[start_processing_counter] if start_processing_counter >= 0 else 0
                        chunk_input_end = input_index_tracker[end_processing_counter - 1] if end_processing_counter > 0 else 0

                        yield caikit_data_model_nlp_pb2.ChunkerTokenizationStreamResult(
                            results=[
                                caikit_data_model_nlp_pb2.Token(
                                    start=abs_start, end=abs_end, text=text
                                )
                            ],
                            input_start_index=chunk_input_start,
                            input_end_index=chunk_input_end,
                            start_index=abs_start,
                            processed_index=abs_end,
                            token_count=1,
                        )

                        # Update processed offset and start counter
                        processed_offset = abs_end
                        start_processing_counter = end_processing_counter

            # Stream iteration complete - yield any remaining chunks
            remaining_chunks = chunker.chunk(accumulated_text[processed_offset:])

            if remaining_chunks:
                for text, start, end in remaining_chunks:
                    abs_start = start + processed_offset
                    abs_end = end + processed_offset

                    if (abs_start, abs_end) in yielded_chunks:
                        continue

                    # Handle leading whitespace for first event
                    if first_event and abs_start != 0:
                        abs_start = 0
                        text = accumulated_text[abs_start:abs_end]
                        first_event = False

                    yielded_chunks.add((abs_start, abs_end))
                    chunk_count += 1

                    chunk_input_start = input_index_tracker[start_processing_counter] if start_processing_counter >= 0 else 0
                    chunk_input_end = input_index_tracker[-1] if input_index_tracker else 0

                    yield caikit_data_model_nlp_pb2.ChunkerTokenizationStreamResult(
                        results=[
                            caikit_data_model_nlp_pb2.Token(
                                start=abs_start, end=abs_end, text=text
                            )
                        ],
                        input_start_index=chunk_input_start,
                        input_end_index=chunk_input_end,
                        start_index=abs_start,
                        processed_index=abs_end,
                        token_count=1,
                    )
            else:
                # Handle whitespace-only or no-chunk case
                # Yield text chunks as-is when no sentences are detected
                sentence_start = 0
                chunk_input_start = input_index_tracker[start_processing_counter] if start_processing_counter >= 0 else 0
                chunk_input_end = input_index_tracker[-1] if input_index_tracker else 0

                for text_chunk in text_tracker:
                    sentence_end = sentence_start + len(text_chunk)
                    chunk_count += 1

                    yield caikit_data_model_nlp_pb2.ChunkerTokenizationStreamResult(
                        results=[
                            caikit_data_model_nlp_pb2.Token(
                                start=sentence_start, end=sentence_end, text=text_chunk
                            )
                        ],
                        input_start_index=chunk_input_start,
                        input_end_index=chunk_input_end,
                        start_index=sentence_start,
                        processed_index=sentence_end,
                        token_count=1,
                    )
                    sentence_start = sentence_end

        except Exception as e:
            logger.error(f"Stream chunking failed: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, str(e))


def serve():
    """Start the gRPC server."""
    interceptors = [LoggingInterceptor()]

    options = [
        # Keepalive:
        ('grpc.http2.min_ping_interval_without_data_ms', 10000),
        ('grpc.keepalive_permit_without_calls', 1),
        ('grpc.keepalive_time_ms', 30000),
        ('grpc.keepalive_timeout_ms', 60000),
        # Resource limits
        ('grpc.http2.max_concurrent_streams', 500),
        ('grpc.max_receive_message_length', 10 * 1024 * 1024),
        ('grpc.max_send_message_length', 10 * 1024 * 1024),
        # Connection lifecycle
        ('grpc.max_connection_age_ms', 30 * 60 * 1000),
        ('grpc.max_connection_idle_ms', 10 * 60 * 1000),
    ]

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=200),
        interceptors=interceptors,
        options=options
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

    # Get available chunkers from registry
    from . import get_chunker_registry

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
