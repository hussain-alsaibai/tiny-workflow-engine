# tiny-workflow-engine

> Zero-dependency async workflow engine with DAG execution, persistence, and retry. For Python AI agents, pipelines, and durable orchestration.

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](tiny_workflow_engine.py)
[![Part of the tiny-* ecosystem](https://img.shields.io/badge/tiny--*-ecosystem-purple.svg)](#ecosystem)

## Why?

- **Celery** — 12+ dependencies, Redis required, heavy broker.
- **Temporal** — Excellent, but Java-flavoured concepts and a separate service.
- **Prefect / Airflow** — Beautiful UIs, but significant setup overhead.

**tiny-workflow-engine** does the 90% case in a single 500-line Python file. DAG-based execution, crash resilience, retry with backoff, async/sync steps, and hooks — zero dependencies.

## Features

| Feature | Description |
|---------|-------------|
| **DAG execution** | Steps run in topological order; deps are enforced |
| **Async + sync** | `async def` and regular `def` steps work seamlessly |
| **Parallel fan-out** | Run multiple steps concurrently; merge results downstream |
| **Persistence** | Checkpoint every step to JSON; resume from last checkpoint on crash |
| **Retry + backoff** | Exponential backoff per step; configurable attempt count |
| **Timeout** | Per-step timeouts with `asyncio.wait_for` |
| **Hooks** | `on_start`, `on_step_complete`, `on_step_fail`, `on_complete`, `on_fail`, `on_cancel` |
| **Cancel** | Signal cancel at step boundary; clean shutdown |
| **Zero deps** | Pure Python stdlib. `asyncio`, `json`, `pathlib` |

## Install

```bash
pip install tiny-workflow-engine
```

Or copy `tiny_workflow_engine.py` — it's one file, zero dependencies.

## Quick Start

```python
import asyncio
from tiny_workflow_engine import WorkflowEngine

engine = WorkflowEngine(persist_dir="./workflows/")

wf = engine.create("my-pipeline")
wf.add_step("fetch", fetch_data)
wf.add_step("process", process_data, deps=["fetch"])
wf.add_step("save", save_results, deps=["process"])

result = await engine.run(wf.workflow_id)
print(result)  # output of the last completed step
```

### AI Agent Pipeline Example

```python
async def agent_pipeline():
    engine = WorkflowEngine(persist_dir="/tmp/workflows/")

    async def fetch_context(input_data, deps):
        return {"context": "relevant docs..."}

    async def generate_response(input_data, deps):
        return {"response": "LLM response..."}

    async def store_memory(input_data, deps):
        return {"stored": True}

    wf = engine.create("agent-run")
    wf.add_step("fetch", fetch_context)
    wf.add_step("generate", generate_response, deps=["fetch"])
    wf.add_step("store", store_memory, deps=["generate"])

    return await engine.run(wf.workflow_id)
```

### Parallel Fan-Out

```python
wf = engine.create("parallel")
wf.add_step("fetch_a", fetch_a)
wf.add_step("fetch_b", fetch_b)
wf.add_step("fetch_c", fetch_c)
wf.add_step("merge", merge_results, deps=["fetch_a", "fetch_b", "fetch_c"])
# merge waits for all three, then runs once
```

### Retry with Backoff

```python
wf.add_step(
    "api_call",
    call_external_api,
    retry_count=3,
    retry_backoff=1.5,  # 0s → 1.5s → 2.25s
    timeout=30.0,
)
```

### Custom Observability Hooks

```python
from tiny_workflow_engine import WorkflowHooks

class AgentHooks(WorkflowHooks):
    def on_step_complete(self, wf, step_id, result):
        print(f"✓ {step_id} → {type(result).__name__}")

    def on_step_fail(self, wf, step_id, error):
        alert(f"Step {step_id} failed: {error}")

    def on_complete(self, wf, snap):
        log_cost(snap)

engine = WorkflowEngine(persist_dir="./workflows/", hooks=AgentHooks())
```

### Resume After Crash

```python
# Workflow crashes at step 3 of 5 — state is saved to disk.
# Fix the bug. Resume from exactly where it left off:
result = engine.resume(workflow_id)
# Steps 1 & 2 are skipped (already completed).
# Step 3 starts fresh.
```

## Architecture

```
WorkflowEngine
  ├── create(name) → Workflow
  │     └── add_step(id, fn, deps=[...])
  │
  ├── run(workflow_id) → result
  │     ├── _topological_sort()     → step execution order
  │     ├── _run_step(step)         → execute + retry + timeout
  │     └── _save_snapshot()       → JSON checkpoint
  │
  ├── cancel(workflow_id)           → signal stop at next boundary
  └── resume(workflow_id)           → restart from last checkpoint
```

## Ecosystem

Part of the **tiny-\*** ecosystem — zero-dependency Python tools for AI agents and developer infrastructure.

| Package | Description |
|---------|-------------|
| [tiny-event-bus](https://github.com/hussain-alsaibai/tiny-event-bus) | Pub/sub with topics, wildcards, DLQ |
| [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent) | ReAct agent framework — one file |
| [tiny-mcp-server](https://github.com/hussain-alsaibai/tiny-mcp-server) | MCP server framework |
| [snapdb](https://github.com/hussain-alsaibai/snapdb) | Lightning-fast embedded database |

## License

MIT © 2026 Hussain Al-Saibai
