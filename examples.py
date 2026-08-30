"""Practical examples for tiny-workflow-engine v0.2.0.

Run any example directly:

    python3 examples.py simple
    python3 examples.py ai
    python3 examples.py fanout
    python3 examples.py map
    python3 examples.py approval
    python3 examples.py all

Each example is a runnable async function. They are intentionally
self-contained — no network, no third-party deps, no on-disk state leaks
between examples (all workflows use ``persist_dir=None`` unless they
demonstrate persistence).
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List

from tiny_workflow_engine import (
    StepStatus,
    WorkflowEngine,
    WorkflowError,
)


# ==========================================================================
# Example 1: Simple pipeline (fetch -> process -> save)
# ==========================================================================
def _fetch(input_data, deps):
    """Pretend to fetch a payload from a source."""
    print("  [fetch] loading payload...")
    return {"raw": [10, 20, 30, 40], "source": input_data.get("source", "memory")}


def _process(input_data, deps):
    """Aggregate the fetched numbers."""
    raw = deps["fetch"]["raw"]
    print(f"  [process] aggregating {len(raw)} samples")
    return {
        "count": len(raw),
        "sum": sum(raw),
        "mean": sum(raw) / len(raw),
    }


def _save(input_data, deps):
    """Pretend to persist the result somewhere."""
    summary = deps["process"]
    print(f"  [save] writing summary: {summary}")
    return {"written": True, "rows": summary["count"]}


async def example_simple_pipeline() -> Dict[str, Any]:
    """A canonical fetch -> process -> save DAG."""
    print("\n=== Example 1: Simple pipeline ===")
    engine = WorkflowEngine(persist_dir=None)
    wf = engine.create("simple-pipeline")
    wf.add_step("fetch", _fetch)
    wf.add_step("process", _process, deps=["fetch"])
    wf.add_step("save", _save, deps=["process"])

    print("\nMermaid diagram:")
    print(wf.to_mermaid())

    results = await engine.run(wf.workflow_id)
    print(f"\nFinal: {results['save']}")
    return results


# ==========================================================================
# Example 2: AI agent pipeline (context fetch -> generate -> store)
# ==========================================================================
async def _fetch_context(input_data, deps):
    """Simulate retrieving RAG context."""
    print("  [fetch_context] retrieving 3 docs from vector store...")
    return {
        "context": [
            "Workflow engines coordinate multi-step AI tasks.",
            "DAGs express dependencies without cycles.",
            "Persistence enables resume after a crash.",
        ],
        "tokens": 142,
    }


async def _generate(input_data, deps):
    """Simulate an LLM call that consumes the context."""
    ctx = deps["fetch_context"]["context"]
    print(f"  [generate] generating answer from {len(ctx)} docs...")
    answer = "Tiny workflow engines run DAGs with persistence and retry."
    return {
        "answer": answer,
        "tokens_in": deps["fetch_context"]["tokens"],
        "tokens_out": len(answer.split()),
    }


def _store_memory(input_data, deps):
    """Simulate writing the agent's reply to long-term memory."""
    gen = deps["generate"]
    print(f"  [store_memory] persisting {gen['tokens_out']}-token reply...")
    return {"stored": True, "tokens": gen["tokens_in"] + gen["tokens_out"]}


async def example_ai_agent_pipeline() -> Dict[str, Any]:
    """An AI agent ReAct-style pipeline: fetch context, generate, store."""
    print("\n=== Example 2: AI agent pipeline ===")
    engine = WorkflowEngine(persist_dir=None)
    wf = engine.create("agent-run")
    wf.add_step("fetch_context", _fetch_context)
    wf.add_step("generate", _generate, deps=["fetch_context"])
    wf.add_step("store_memory", _store_memory, deps=["generate"])

    print("\nMermaid diagram:")
    print(wf.to_mermaid())

    results = await engine.run(wf.workflow_id)
    print(f"\nFinal: {results['store_memory']}")
    return results


# ==========================================================================
# Example 3: Parallel fan-out (fetch from 3 sources concurrently)
# ==========================================================================
def _fetch_a(input_data, deps):
    return {"service": "api-a", "latency_ms": 120, "status": "ok"}


def _fetch_b(input_data, deps):
    return {"service": "api-b", "latency_ms": 85, "status": "ok"}


def _fetch_c(input_data, deps):
    return {"service": "api-c", "latency_ms": 200, "status": "ok"}


async def example_parallel_fanout() -> Dict[str, Any]:
    """Run three fetches in parallel via add_parallel, then aggregate."""
    print("\n=== Example 3: Parallel fan-out ===")
    engine = WorkflowEngine(persist_dir=None)
    wf = engine.create("parallel-fanout")

    # add_parallel fans out three sync functions concurrently and gathers
    # them into a single list keyed under the parent step's name.
    wf.add_parallel("fetch", [_fetch_a, _fetch_b, _fetch_c])

    # A downstream step can read the gathered list via deps["fetch"].
    def aggregate(input_data, deps):
        sources = deps["fetch"]
        total = sum(s["latency_ms"] for s in sources)
        print(f"  [aggregate] total latency across {len(sources)} sources: {total}ms")
        return {
            "sources": [s["service"] for s in sources],
            "total_latency_ms": total,
        }

    wf.add_step("aggregate", aggregate, deps=["fetch"])

    print("\nMermaid diagram:")
    print(wf.to_mermaid())

    results = await engine.run(wf.workflow_id)
    print(f"\nFinal: {results['aggregate']}")
    return results


# ==========================================================================
# Example 4: Map (process a list of items in parallel)
# ==========================================================================
async def example_map_items() -> Dict[str, Any]:
    """Fan a callback out over a list of items via add_map."""
    print("\n=== Example 4: Map over items ===")
    engine = WorkflowEngine(persist_dir=None)
    wf = engine.create("map-items")

    async def enrich(input_data, item):
        # Pretend each item is an ID we'd look up in a database.
        await asyncio.sleep(0.01)  # simulate I/O
        return {"id": item, "name": f"user-{item}", "tier": "gold" if item % 2 else "silver"}

    wf.add_map("enrich_users", enrich, items=[1, 2, 3, 4, 5])

    print("\nMermaid diagram:")
    print(wf.to_mermaid())

    results = await engine.run(wf.workflow_id)
    print(f"\nEnriched {len(results['enrich_users'])} users")
    for u in results["enrich_users"]:
        print(f"  - {u}")
    return results


# ==========================================================================
# Example 5: Approval gate workflow (human-in-the-loop)
# ==========================================================================
async def example_approval_gate() -> Dict[str, Any]:
    """Human-in-the-loop deploy: build/test pass, deploy requires approval.

    Run with ``APPROVE_DEPLOY_PROD=1`` to satisfy the gate via the env
    var; otherwise place an approval file under the persist-dir and re-run.
    """
    print("\n=== Example 5: Approval gate (human-in-the-loop) ===")
    tmpdir = tempfile.mkdtemp(prefix="twf-approval-")
    engine = WorkflowEngine(persist_dir=tmpdir)
    wf = engine.create("deploy")
    wf.add_step("build", lambda input_data, deps: {"artifact": "app.zip"})
    wf.add_step("test", lambda input_data, deps: {"passed": True}, deps=["build"])

    def deploy_prod(input_data, deps):
        print("  [deploy_prod] 🚀 deploying to prod...")
        return {"url": "https://app.example.com", "version": "1.0.0"}

    wf.add_step(
        "deploy_prod",
        deploy_prod,
        deps=["test"],
        step_type="approval",  # <-- the gate
    )

    print("\nMermaid diagram:")
    print(wf.to_mermaid())

    # First run: deploy_prod should be SKIPPED.
    print("\nRun 1 (no approval yet):")
    results = await engine.run(wf.workflow_id)
    snap = engine._load_snapshot(wf.name)
    for name, step in snap.steps.items():
        status = step["status"].value if hasattr(step["status"], "value") else step["status"]
        print(f"  {name}: {status}")

    # Now flip the env var and re-run. The gate will be re-evaluated.
    print("\nApproving via env var APPROVE_DEPLOY_PROD=1...")
    os.environ["APPROVE_DEPLOY_PROD"] = "1"
    try:
        results = await engine.run(wf.workflow_id)
    finally:
        os.environ.pop("APPROVE_DEPLOY_PROD", None)
    snap = engine._load_snapshot(wf.name)
    for name, step in snap.steps.items():
        status = step["status"].value if hasattr(step["status"], "value") else step["status"]
        print(f"  {name}: {status}")
    print(f"\nFinal deploy result: {results.get('deploy_prod')}")

    # Cleanup the temp dir.
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)
    return results


# ==========================================================================
# Runner
# ==========================================================================
async def _run_all() -> None:
    await example_simple_pipeline()
    await example_ai_agent_pipeline()
    await example_parallel_fanout()
    await example_map_items()
    await example_approval_gate()


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] == "all":
        asyncio.run(_run_all())
        return

    # Map CLI names to actual example_* function names.
    aliases = {
        "simple": "example_simple_pipeline",
        "ai": "example_ai_agent_pipeline",
        "fanout": "example_parallel_fanout",
        "map": "example_map_items",
        "approval": "example_approval_gate",
        "all": "_run_all",
    }
    target = aliases.get(sys.argv[1])
    if target is None:
        print(f"Unknown example: {sys.argv[1]!r}")
        print("Available: simple, ai, fanout, map, approval, all")
        sys.exit(1)
    fn = globals().get(target)
    if fn is None:
        print(f"Could not resolve example: {target}")
        sys.exit(1)
    if target == "_run_all":
        asyncio.run(fn())
    else:
        asyncio.run(fn())


if __name__ == "__main__":
    main()
