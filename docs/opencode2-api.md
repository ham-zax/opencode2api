# OpenCode2 Zen API capture

Captured from the installed `opencode2` wrapper and OpenCode **2.0.21** on 2026-10-02. An isolated configuration pointed only the `opencode` provider at a loopback recorder, which forwarded to `https://opencode.ai/zen/v1`. Both native CLI runs returned `OK` with HTTP 200. Authorization was redacted; prompt content and credentials are not stored here.

| Native model | HTTP endpoint | Request fields observed |
| --- | --- | --- |
| `big-pickle` | `POST /v1/chat/completions` | `model`, `messages`, `tools`, `stream`, `stream_options`, `max_completion_tokens` |
| `muse-spark-1.3-contributor-free` | `POST /v1/responses` | `model`, `input`, `instructions`, `tools`, `store`, `prompt_cache_key`, `include`, `max_output_tokens`, `stream` |

Native requests used `User-Agent: opencode/latest/2.0.21/cli`, `x-opencode-client: cli`, a project ID, and the same canonical session ID in `x-opencode-session`, `x-opencode-session-id`, `x-session-affinity`, and `x-session-id`. The session format was `ses_` followed by 12 lowercase hexadecimal characters and 14 alphanumeric characters. Requests streamed; Chat declared nested function tools and Responses declared flat function tools.

The native provider API identified Zen as `opencode` and Go as `opencode-go`. Its active free Zen catalog at capture time contained Big Pickle, Fledge Alpha, LongCat 2.5 Preview, Space Bunny, MiMo 2.6 Flash, Muse Spark 1.3 Contributor, Ling 3.0 Flash Fin, Nemotron 3 Ultra, and Nemotron 3.5 Lightning. Muse used the OpenAI Responses transport; the others used OpenAI-compatible Chat. Ling's catalog membership does not establish availability: the live endpoint rejected Ling 3.0 Flash Fin with HTTP 400.

The installed public model registry marked `deepseek-v4-flash-free`, `muse-spark-1.2-contributor-free`, and `mimo-v2.5-free` deprecated. Jev is a Zen structured-decision model served at `/v1/systemone`; the gateway supports text generation on Chat and Responses. Model names can overlap between Zen and Go; namespace and pricing determine eligibility.

[The sanitized fixture](../tests/fixtures/opencode2-zen.json) preserves the observed request structure, native catalog selection, and public metadata. A regression test compares discovery and endpoint selection with this capture. The gateway refreshes current [models.dev metadata](https://models.dev/api.json) and [Zen's advertised models](https://opencode.ai/zen/v1/models); the fixture is evidence from one capture, not a fixed production model list. During the final live gateway check, discovery also picked up the newly advertised `ling-3.1-flash-free` without a code change.

To inspect a running installation, use `opencode2 api provider.list` and `opencode2 api model.list`. Private servers need time to initialize their catalog before these calls return populated lists. Raw provider/model settings may contain credentials; redact them before saving or sharing. Upstream protocol documentation: [Zen](https://opencode.ai/docs/zen/) and [Go](https://opencode.ai/docs/go/).
