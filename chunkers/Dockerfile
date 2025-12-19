FROM registry.access.redhat.com/ubi9/python-312:latest

WORKDIR /app

# Install dependencies
COPY chunkers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY --chown=1001:0 protos /app/protos
COPY --chown=1001:0 chunkers /app/chunkers

# Generate protobuf files
RUN python -m grpc_tools.protoc \
    -I/app/protos \
    --python_out=/app/chunkers \
    --grpc_python_out=/app/chunkers \
    /app/protos/caikit_data_model_nlp.proto /app/protos/chunkers.proto

# Fix import statements in generated protobuf files
RUN sed -i 's/^import caikit_data_model_nlp_pb2/from . import caikit_data_model_nlp_pb2/' /app/chunkers/chunkers_pb2.py && \
    sed -i 's/^import caikit_data_model_nlp_pb2/from . import caikit_data_model_nlp_pb2/' /app/chunkers/chunkers_pb2_grpc.py && \
    sed -i 's/^import chunkers_pb2/from . import chunkers_pb2/' /app/chunkers/chunkers_pb2_grpc.py

EXPOSE 8085

ENV PYTHONPATH=/app

CMD ["python", "-m", "chunkers.grpc_server"]