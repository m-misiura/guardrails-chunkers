# Instructions

- create a new namespace, e.g. called test

```bash
oc new-project test
```

- deploy llm 

```bash
oc apply -f demo/llm
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

```bash
oc apply -f demo/orchestrator
```

## Sample requests

- Get the route for the orchestrator

```bash
ORCHESTRATOR_ROUTE="https://$(oc get routes guardrails-orchestrator -o jsonpath='{.spec.host}')"
```

## Harmless request -- should pass in put and output

```bash
curl -X POST $ORCHESTRATOR_ROUTE/api/v2/chat/completions-detection \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "llm",
    "messages": [
      {
        "role": "user",
        "content": "Can you provide a nice V60 coffee recipe?"
      }
    ],
    "stream": true,
    "detectors": {
      "input": {
        "hap": {},
        "built-in-detector": {}
      },
      "output": {
        "built-in-detector": {},
        "hap": {}
      }
    }
  }'
```

this should return something like

```
data: {"id":"chat-f612bfeb1ed0425cb07e7c468becd43d","object":"chat.completion.chunk","created":1765384660,"model":"llm","choices":[{"index":0,"delta":{"role":"assistant","content":""},"logprobs":null,"finish_reason":null,"stop_reason":null}],"detections":{"output":[{"choice_index":0,"results":[]}]}}

data: {"id":"chat-f612bfeb1ed0425cb07e7c468becd43d","object":"chat.completion.chunk","created":1765384660,"model":"llm","choices":[{"index":0,"delta":{"role":"assistant","content":"Sure!"},"logprobs":null,"finish_reason":null,"stop_reason":null}],"detections":{"output":[{"choice_index":0,"results":[]}]}}

data: {"id":"chat-f612bfeb1ed0425cb07e7c468becd43d","object":"chat.completion.chunk","created":1765384660,"model":"llm","choices":[{"index":0,"delta":{"role":"assistant","content":"Sure! Here's a delicious V60 coffee recipe that you can enjoy! 🌿\n\n### V60 Coffee Recipe\n\n#### Ingredients:\n- 2 tablespoons of steamed or roasted black tea\n- 1 tablespoon of steamed or roasted green coffee beans\n- 1 teaspoon of sugar\n- 1 teaspoon of salt\n- 1 tablespoon of honey or maple syrup (optional)\n- 1 tablespoon of vanilla extract\n- 1 tablespoon of espresso powder (optional)\n\n#### Equipment:\n- A French press or espresso machine\n- A moka pot or other large pot for brewing\n- A coffee filter\n- A strainer\n- A coffee spoon\n- A coffee mug\n\n#### Method:\n1. **Preparation:**\n   - Heat a mug of water in a Dutch oven or large pot over low heat."},"logprobs":null,"finish_reason":null,"stop_reason":null}],"detections":{"output":[{"choice_index":0,"results":[]}]}}

data: {"id":"chat-f612bfeb1ed0425cb07e7c468becd43d","object":"chat.completion.chunk","created":1765384660,"model":"llm","choices":[{"index":0,"delta":{"role":"assistant","content":"Add a few drops of sugar and let it sit for a few minutes to infuse the water.\n   - Pour the hot water into the French press or espresso machine."},"logprobs":null,"finish_reason":null,"stop_reason":null}],"detections":{"output":[{"choice_index":0,"results":[]}]}}

data: {"id":"chat-f612bfeb1ed0425cb07e7c468becd43d","object":"chat.completion.chunk","created":1765384660,"model":"llm","choices":[{"index":0,"delta":{"role":"assistant","content":"Press down on the plunger and let the water flow through the coffee grounds for about 30 seconds to concentrate the coffee.\n\n2. **Pouring the Coffee:**\n   - After the coffee has stood for about 30 seconds, pour the coffee into a moka pot or large pot.\n   - Add your steamed or roasted black tea, green coffee beans, and salt to the pot."},"logprobs":null,"finish_reason":null,"stop_reason":null}],"detections":{"output":[{"choice_index":0,"results":[]}]}}

data: {"id":"chat-f612bfeb1ed0425cb07e7c468becd43d","object":"chat.completion.chunk","created":1765384660,"model":"llm","choices":[{"index":0,"delta":{"role":"assistant","content":"Stir to combine.\n   - Add about 1 tablespoon of honey or maple syrup, if using, and stir well.\n   - Add 1 tablespoon of vanilla extract, if using, and stir gently.\n   - Pour the hot coffee into a mug and enjoy immediately!\n\n3. **Caramelizing:**\n   - Let the coffee cool for a few minutes to solidify the caramel flavor.\n\n#### Notes:\n- V60-style coffee pods can add a subtle sweetness and a good brown flavor.\n- For a richer flavor, try adding more black tea or using a stronger coffee concentrate.\n- A shot of espresso can help balance the sweetness and depth of the coffee."},"logprobs":null,"finish_reason":null,"stop_reason":null}],"detections":{"output":[{"choice_index":0,"results":[]}]}}

data: {"id":"chat-f612bfeb1ed0425cb07e7c468becd43d","object":"chat.completion.chunk","created":1765384660,"model":"llm","choices":[{"index":0,"delta":{"role":"assistant","content":"Enjoy your V60 coffee!"},"logprobs":null,"finish_reason":null,"stop_reason":null}],"detections":{"output":[{"choice_index":0,"results":[]}]}}

data: {"id":"chat-f612bfeb1ed0425cb07e7c468becd43d","object":"chat.completion.chunk","created":1765384660,"model":"llm","choices":[{"index":0,"delta":{"role":"assistant","content":""},"logprobs":null,"finish_reason":"stop","stop_reason":null}]}

data: [DONE]
```

## Harmful request -- should be blocked in input

```bash
curl -X POST $ORCHESTRATOR_ROUTE/api/v2/chat/completions-detection \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "llm",
    "messages": [
      {
        "role": "user",
        "content": "You dotard I really hate this"
      }
    ],
    "stream": true,
    "detectors": {
      "input": {
        "hap": {},
        "built-in-detector": {}
      },
      "output": {
        "built-in-detector": {},
        "hap": {}
      }
    }
  }'
```

this should return something like

```
data: {"id":"f75060ad484c41b8ba48ad062b691658","object":"chat.completion.chunk","created":1765384744,"model":"llm","choices":[],"usage":{"prompt_tokens":7,"total_tokens":0,"completion_tokens":0},"detections":{"input":[{"message_index":0,"results":[{"start":0,"end":29,"text":"You dotard I really hate this","detector_id":"hap","detection_type":"LABEL_1","detection":"single_label_classification","score":0.949025809764862}]}]},"warnings":[{"type":"UNSUITABLE_INPUT","message":"Unsuitable input detected. Please check the detected entities on your input and try again with the unsuitable input removed."}]}

data: [DONE]
```