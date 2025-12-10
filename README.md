# Guardrails chunkers

Chunker server implementation(s) to be invoked by the orchestrator for performing detections on text generation input and output

## Available chunkers

Currently, the following chunkers are available:

- sentence, 
- langchain_recursive_character, 
- langchain_character

## Build container

To build the chunker container on a Mac, run the following command from the root of the repository:

```bash
podman build --platform linux/amd64 -f chunkers/Dockerfile -t <INSERT YOUR REPO NAME:TAG> .
```

## Run as part of the orchestrator setup

Naviagate to the `demo` subdirectory and follow the instructions in the `README.md` file there to run the orchestrator with the chunker included.