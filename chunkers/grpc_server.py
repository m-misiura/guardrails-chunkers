import asyncio
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


class LoggingInterceptor(grpc.aio.ServerInterceptor):
    """Interceptor to log all gRPC requests."""

    async def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method
        logger.info(f"gRPC request received: {method}")

        try:
            response = await continuation(handler_call_details)
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
        """Streaming chunking request."""
        try:
            metadata = dict(context.invocation_metadata())
            model_id = metadata.get("mm-model-id", "sentence")

            logger.info(f"Received streaming chunking request: model_id={model_id}")

            chunker = self.registry.get(model_id)
            if not chunker:
                logger.error(
                    f"Unknown chunker: {model_id}. Available: {self.registry.list_names()}"
                )
                context.abort(grpc.StatusCode.NOT_FOUND, f"Unknown chunker: {model_id}")

            chunk_count = 0
            for request in request_iterator:
                logger.debug(
                    f"Processing stream chunk: input_index={request.input_index_stream}, text_length={len(request.text_stream)}"
                )
                chunks = chunker.chunk(request.text_stream)

                for text, start, end in chunks:
                    chunk_count += 1
                    logger.debug(
                        f"Yielding chunk {chunk_count}: start={start}, end={end}, text={text[:50]}"
                    )
                    yield caikit_data_model_nlp_pb2.ChunkerTokenizationStreamResult(
                        results=[
                            caikit_data_model_nlp_pb2.Token(
                                start=start, end=end, text=text
                            )
                        ],
                        input_start_index=request.input_index_stream,
                        input_end_index=request.input_index_stream,
                        start_index=start,
                        processed_index=end,
                        token_count=1,
                    )

            logger.info(
                f"Streaming complete: model_id={model_id}, total_chunks={chunk_count}"
            )

        except Exception as e:
            logger.error(f"Stream chunking failed: {e}", exc_info=True)
            context.abort(grpc.StatusCode.INTERNAL, str(e))


async def serve():
    """Start the gRPC server."""
    interceptors = [LoggingInterceptor()]
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=10), interceptors=interceptors
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
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
