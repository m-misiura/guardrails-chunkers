# Instructions

- create a new namespace, e.g. called llm-d

```bash
oc new-project llm-d
```

- deploy llm with llm-d

```bash
oc apply -f demo/llm/llm-d/deployment.yaml 
```

- deploy chunker

```bash
oc apply -f demo/chunkers
```

- deploy detector

```bash
oc apply -f demo/detectors
```

- deploy orchestrator


- extract cert and key from KServe secret

```bash
oc get secret qwen2-05b-instruct-single-kserve-self-signed-certs -n llm-d \
  -o jsonpath='{.data.tls\.crt}' | base64 -d > tls.crt

oc get secret qwen2-05b-instruct-single-kserve-self-signed-certs -n llm-d \
  -o jsonpath='{.data.tls\.key}' | base64 -d > tls.key
```


- create a new secret with certs for orchestrator

```bash
oc create secret generic qwen-ca-tls -n llm-d \
  --from-file=tls.crt=tls.crt \
  --from-file=tls.key=tls.key \
  --from-file=ca.crt=tls.crt
```

```bash
oc apply -f demo/orchestrator/orch-gateway-llmd.yaml
```


## Guardrails Gateway

- get the route for the gateway

```bash
GUARDRAILS_GATEWAY=https://$(oc get routes guardrails-orchestrator-gateway -o jsonpath='{.spec.host}')
```

- send a request to the gateway via the `passthrough` endpoint, which bypasses guardrails processing and sends directly to the LLM

```bash
curl POST $GUARDRAILS_GATEWAY/passthrough/v1/chat/completions -v \
-H "Authorization: Bearer $(oc whoami -t)" \
-H "Content-Type: application/json" \
-d '{
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "messages": [
        {
            "role": "user",
            "content": "Can you provide a nice V60 coffee recipe?"
        }
    ],
    "stream": true
}'
```

- send a request to the gateway via the `all` endpoint, which processes the request with guardrails


- first trigger hap detector on input: 

```bash
curl -X POST "$GUARDRAILS_GATEWAY/all/v1/chat/completions" -v \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "messages": [
      {
        "role": "user",
        "content": "You dotard I really flipping hate you"
      }
    ],
    "stream": true
  }'
```

which should yield:

```
data: {"id":"7af5d99e7c644f0eb44e1cae7872d36d","object":"chat.completion.chunk","created":1765979000,"model":"Qwen/Qwen2.5-0.5B-Instruct","choices":[],"usage":{"completion_tokens":0,"prompt_tokens":8,"total_tokens":0},"detections":{"input":[{"message_index":0,"results":[{"start":0,"end":37,"text":"You dotard I really flipping hate you","detection_type":"LABEL_1","detection":"single_label_classification","detector_id":"hap","score":0.8138196468353271}]}],"output":null},"warnings":[{"type":"UNSUITABLE_INPUT","message":"Unsuitable input detected. Please check the detected entities on your input and try again with the unsuitable input removed."}]}

data: 
```

- second trigger regex detector on input: 

```bash
curl -X POST "$GUARDRAILS_GATEWAY/all/v1/chat/completions" -v \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "messages": [
      {
        "role": "user",
        "content": "My email is test@example.com"
      }
    ],
    "stream": true
  }'
```

which should yield:

```
data: {"id":"9b3bbe7ac6234dde9789ce3bca4a737c","object":"chat.completion.chunk","created":1765979057,"model":"Qwen/Qwen2.5-0.5B-Instruct","choices":[],"usage":{"completion_tokens":0,"prompt_tokens":6,"total_tokens":0},"detections":{"input":[{"message_index":0,"results":[{"start":12,"end":28,"text":"test@example.com","detection_type":"pii","detection":"email_address","detector_id":"built-in-detector","score":1.0}]}],"output":null},"warnings":[{"type":"UNSUITABLE_INPUT","message":"Unsuitable input detected. Please check the detected entities on your input and try again with the unsuitable input removed."}]}

data: 

* Connection #0 to host guardrails-orchestrator-gateway-llm-d.apps.rosa.trustyai-mac.v5di.p3.openshiftapps.com left intact
```

- third send a clean input:

```bash
curl -X POST "$GUARDRAILS_GATEWAY/all/v1/chat/completions" -v \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "messages": [
      {
        "role": "user",
        "content": "Write three sentences about why lemons are great"
      }
    ],
    "stream": true
  }'
```

which should yield

```
data: {"id":"chatcmpl-20ef1d6a235141ddaf446d8a1ad11d7d","object":"chat.completion.chunk","created":1765979124,"model":"Qwen/Qwen2.5-0.5B-Instruct","choices":[{"index":0,"delta":{"content":"","role":"assistant","tool_calls":null},"logprobs":null,"finish_reason":null,"stop_reason":null}],"usage":null,"detections":{"input":null,"output":[{"choice_index":0,"results":[]}]},"warnings":null}

data: {"id":"chatcmpl-20ef1d6a235141ddaf446d8a1ad11d7d","object":"chat.completion.chunk","created":1765979124,"model":"Qwen/Qwen2.5-0.5B-Instruct","choices":[{"index":0,"delta":{"content":"Lemons are incredibly versatile and healthy for your body, making them a great choice when it comes to lemonade or lemon juice.","role":"assistant","tool_calls":null},"logprobs":null,"finish_reason":null,"stop_reason":null}],"usage":null,"detections":{"input":null,"output":[{"choice_index":0,"results":[]}]},"warnings":null}

data: {"id":"chatcmpl-20ef1d6a235141ddaf446d8a1ad11d7d","object":"chat.completion.chunk","created":1765979124,"model":"Qwen/Qwen2.5-0.5B-Instruct","choices":[{"index":0,"delta":{"content":"They also have natural antibacterial properties that can help reduce the risk of infections.","role":"assistant","tool_calls":null},"logprobs":null,"finish_reason":null,"stop_reason":null}],"usage":null,"detections":{"input":null,"output":[{"choice_index":0,"results":[]}]},"warnings":null}

data: {"id":"chatcmpl-20ef1d6a235141ddaf446d8a1ad11d7d","object":"chat.completion.chunk","created":1765979124,"model":"Qwen/Qwen2.5-0.5B-Instruct","choices":[{"index":0,"delta":{"content":"Additionally, lemons contain essential oils that can be used in aromatherapy to promote relaxation and stress relief.","role":"assistant","tool_calls":null},"logprobs":null,"finish_reason":null,"stop_reason":null}],"usage":null,"detections":{"input":null,"output":[{"choice_index":0,"results":[]}]},"warnings":null}

data: {"id":"chatcmpl-20ef1d6a235141ddaf446d8a1ad11d7d","object":"chat.completion.chunk","created":1765979124,"model":"Qwen/Qwen2.5-0.5B-Instruct","choices":[{"index":0,"delta":{"content":"Overall, lemons offer a range of benefits from their refreshing taste to their health-promoting qualities.","role":"assistant","tool_calls":null},"logprobs":null,"finish_reason":null,"stop_reason":null}],"usage":null,"detections":{"input":null,"output":[{"choice_index":0,"results":[]}]},"warnings":null}
```