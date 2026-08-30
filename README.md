# tiny-workflow-engine

> Zero-dependency async workflow engine with DAG execution, persistence, retry, approval gates, and Mermaid export. For Python AI agents, pipelines, and durable orchestration.

[![Version v0.2.0](https://img.shields.io/badge/version-0.2.0-blue.svg)](https://github.com/hussain-alsaibai/tiny-workflow-engine/releases)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](tiny_workflow_engine.py)
[![Part of the tiny-* ecosystem](https://img.shields.io/badge/tiny--*-ecosystem-purple.svg)](#ecosystem)

## Why?

- **Celery** — 12+ dependencies, Redis required, heavy broker.
- **Temporal** — Excellent, but Java-flavoured concepts and a separate service.
- **Prefect / Airflow** — Beautiful UIs, but significant setup overhead.

**tiny-workflow-engine** does the 90% case in a single pure-Python file. DAG-based execution, crash resilience, retry with backoff, async/sync steps, approval gates, Mermaid export, and a CLI — zero dependencies.

## Workflow at a Glance

```
       ┌──────────┐     ┌───────────┐     ┌──────────┐
       │  fetch   │────▶│  process  │────▶│   save   │
       └──────────┘     └───────────┘     └──────────┘
                             │
                             ▼
                       ┌───────────┐
                       │   audit   │   (approval gate)
                       └───────────┘
```

## Features

| Feature | Description |
|---------|-------------|
| **DAG execution** | Steps run in topological order; deps are enforced |
| **Async + sync** | `async def` and regular `def` steps work seamlessly |
| **Parallel fan-out** | Run multiple steps concurrently; merge results downstream |
| **Persistence** | Checkpoint every step to JSON; resume from last checkpoint on crash |
| **Retry + backoff** | Exponential backoff per step; configurable attempt count |
| **Timeout** | Per-step timeouts with `asyncio.wait_for`; optional global timeout |
| **Hooks** | `on_start`, `on_step_start`, `on_step_complete`, `on_step_fail`, `on_complete`, `on_fail` |
| **Map** | Fan a callback out over a list of items concurrently |
| **Approval gates** | Human-in-the-loop steps gated by env vars or approval files |
| **Mermaid export** | `wf.to_mermaid()` returns a `flowchart LR` graph for docs/PRs |
| **CLI** | `twf --list`, `--status`, `--approve`, `--dot` |
| **Zero deps** | Pure Python stdlib. `asyncio`, `json`, `pathlib` |

## Install

```bash
pip install tiny-workflow-engine
```

Or copy `tiny_workflow_engine.py` (and optionally `cli.py`) — zero dependencies.

## Quick Start

```python
import asyncio
from tiny_workflow_engine import WorkflowEngine

engine = WorkflowEngine(persist_dir="./workflows/")

wf = engine.create("my-pipeline")
wf.add_step("fetch", fetch_data)
wf.add_step("process", process_data, deps=["fetch"])
wf.add_step("save", save_results, deps=["process"])

result = await engine.run("my-pipeline")
# result = {"fetch": ..., "process": ..., "save": ...} — keyed by step name
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
        log_cost(snap.summary)  # v0.2.0: rich summary dict

engine = WorkflowEngine(persist_dir="./workflows/", hooks=AgentHooks())
```

### Resume After Crash

```python
# Workflow crashes at step 3 of 5 — step state is checkpointed to JSON after
# every step and on every failure. Fix the bug, redeclare the same DAG, and
# run again: completed steps are skipped, the failed step retries fresh.
result = await engine.run("my-pipeline")
# steps 1 & 2 are skipped (already completed); step 3 starts fresh.
```

Run `python examples.py` for five end-to-end demos: a simple fetch→process→save
pipeline, an AI agent pipeline, parallel fan-out, `add_map` over a list, and a
human-in-the-loop approval gate.

## Approval Gates

Human-in-the-loop workflows are a first-class concept: any step declared with
`step_type="approval"` is gated. The engine checks for an approval file or a
matching environment variable at run-time. If neither is present, the step is
marked `SKIPPED` and downstream steps still execute with the gate's result
slot set to `None`.

```python
wf = engine.create("deploy")
wf.add_step("build", build_artifact)
wf.add_step("test", run_tests, deps=["build"])
wf.add_step(
    "deploy_prod",
    deploy_to_prod,
    deps=["test"],
    step_type="approval",   # <- this step is gated
)
```

### Approve via environment variable

```bash
APPROVE_DEPLOY_PROD=1 python my_workflow.py
```

The variable name is `APPROVE_<STEP_NAME>` with the step name upper-cased and
non-identifier characters (`-`, spaces) preserved.

### Approve via approval file

When the engine is created with `persist_dir="/var/lib/workflows"`, the gate
looks for:

```
/var/lib/workflows/approvals/<workflow_id>/<step_name>.approved
```

where `<workflow_id>` is the `name:sequence` identifier (e.g. `deploy:1`).
The CLI creates this file for you:

```bash
twf --approve deploy/deploy_prod --persist-dir /var/lib/workflows
```

### Re-running a previously-skipped gate

Approval-gate steps are **re-evaluated on every run**. If the previous run
recorded `deploy_prod` as `SKIPPED`, the next run resets the gate to `PENDING`
and re-checks the approval sources. Once approved, the step runs and the rest
of the workflow proceeds normally.

## Mermaid Export

Render any workflow as a Mermaid `flowchart` for docs, PR descriptions, or
chat:

```python
print(wf.to_mermaid())
```

Output for a linear pipeline:

```
flowchart LR
    fetch
    fetch --> process
    process --> save
```

For a parallel fan-out:

```
flowchart LR
    fetch_a
    fetch_b
    fetch_c
    fetch_a --> merge
    fetch_b --> merge
    fetch_c --> merge
```

Paste the output into any Mermaid-aware renderer (GitHub Markdown, GitLab,
Notion, Obsidian, etc.) to visualize the DAG.

## CLI

Installing the package exposes the `twf` command:

```bash
# List every persisted workflow under the given directory.
twf --list --persist-dir /var/lib/workflows

# Show the status (state, per-step status, durations, summary) of one workflow.
twf --status my-pipeline --persist-dir /var/lib/workflows

# Approve a pending approval-gate step.
twf --approve my-pipeline/deploy_prod --persist-dir /var/lib/workflows

# Print the Mermaid flowchart for a workflow (read from its persisted snapshot).
twf --dot my-pipeline --persist-dir /var/lib/workflows
```

The CLI is implemented in `cli.py` and re-exported from `tiny_workflow_engine`
as the `cli` symbol so the `twf` console script in `pyproject.toml` resolves
to it without polluting the library namespace.

## Architecture

```
   tiny-workflow-engine
   ┌─────────────────────────────────────────────────────┐
   │ WorkflowEngine                                      │
   │   ├── create(name) → Workflow                       │
   │   │     └── add_step / add_parallel / add_map       │
   │   │                                                 │
   │   ├── run(name_or_id) → {step: result}              │
   │   │     ├── _topological_sort()   → step order      │
   │   │     ├── _approval_gate()      → check approval  │
   │   │     ├── _execute(step)        → run + retry     │
   │   │     └── _save_snapshot()      → JSON checkpoint │
   │   │                                                 │
   │   ├── resume(workflow_id)         → restart from    │
   │   │                                 last checkpoint │
   │   │                                                 │
   │   └── to_mermaid()                → DAG visualization│
   │                                                     │
   │ WorkflowSnapshot.summary → human-readable metrics   │
   └─────────────────────────────────────────────────────┘
                              ▲
                              │  twf (CLI)
                              │
   ┌─────────────────────────────────────────────────────┐
   │ cli.py                                              │
   │   --list     → enumerate persisted snapshots       │
   │   --status   → render snapshot summary              │
   │   --approve  → write approval file                  │
   │   --dot      → Mermaid flowchart from snapshot      │
   └─────────────────────────────────────────────────────┘
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
