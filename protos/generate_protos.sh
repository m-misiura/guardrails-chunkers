#!/bin/bash
python -m grpc_tools.protoc \
  -I./protos \
  --python_out=./chunkers \
  --grpc_python_out=./chunkers \
  --pyi_out=./chunkers \
  protos/caikit_data_model_nlp.proto protos/chunkers.proto