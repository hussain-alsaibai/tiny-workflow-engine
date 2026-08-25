"""Practical examples for tiny-workflow-engine.

Run with:  python examples.py
"""

import asyncio

from tiny_workflow_engine import WorkflowEngine, WorkflowError


# --------------------------------------------------------------------------
# Example 1: Multi-step AI agent pipeline (fetch -> analyze -> store)
# --------------------------------------------------------------------------
def fetch_news(input_data, deps):
    print("[1] Fetching latest news...")
    return [
        {"headline": "AI adoption accelerates", "source": "Reuters"},
        {"headline": "Workflow engines go mainstream", "source": "TechCrunch"},
        {"headline": "Python stays in the top 3", "source": "GitHub Blog"},
    ]


def analyze_sentiment(input_data, deps):
    articles = deps["fetch_news"]
    print(f"[2] Analyzing {len(articles)} articles...")
    return [
        {**a, "sentiment": 0.9 if "AI" in a["headline"] else 0.4}
        for a in articles
    ]


def store_results(input_data, deps):
    analyzed = deps["analyze_sentiment"]
    print(f"[3] Storing {len(analyzed)} enriched articles to DB...")
    return {"stored": len(analyzed)}


async def example_1_ai_agent_pipeline():
    print("\n=== Example 1: AI agent pipeline ===")
    engine = WorkflowEngine(persist_dir=None)
    wf = engine.create("news-sentiment-agent")

    wf.add_step("fetch_news", fetch_news)
    wf.add_step("analyze_sentiment", analyze_sentiment, deps=["fetch_news"])
    wf.add_step("store_results", store_results, deps=["analyze_sentiment"])

    results = await engine.run(wf.workflow_id)
    print(f"Pipeline finished. Stored {results['store_results']['stored']} records.")


# --------------------------------------------------------------------------
# Example 2: Parallel execution with a barrier (merge)
# --------------------------------------------------------------------------
def fetch_source_one(input_data, deps):
    return {"service": "api-a", "latency_ms": 120}


def fetch_source_two(input_data, deps):
    return {"service": "api-b", "latency_ms": 85}


def fetch_source_three(input_data, deps):
    return {"service": "api-c", "latency_ms": 200}


def merge_sources(input_data, deps):
    sources = deps["fetch"]  # gathered list from the parallel parent step
    print(f"[Parallel] Merging {len(sources)} endpoints...")
    return {
        "total_latency": sum(s["latency_ms"] for s in sources),
        "endpoints": sorted(s["service"] for s in sources),
    }


async def example_2_parallel_barrier():
    print("\n=== Example 2: Parallel fan-out with barrier ===")
    engine = WorkflowEngine(persist_dir=None)
    wf = engine.create("parallel-merge")

    # add_parallel fans out three functions concurrently, then gathers.
    wf.add_parallel(
        "fetch",
        [fetch_source_one, fetch_source_two, fetch_source_three],
    )
    # A separate merge step depends on the parallel gather's result.
    wf.add_step("merge", merge_sources, deps=["fetch"])

    results = await engine.run(wf.workflow_id)
    print(f"Merged: {results['merge']}")


# --------------------------------------------------------------------------
# Example 3: Resume from a checkpoint after a simulated crash
# --------------------------------------------------------------------------
def download_step(input_data, deps):
    return {"stage": "download", "bytes": 4096}


def transform_step(input_data, deps):
    if not input_data.get("resume_ok", False):
        # Simulate a crash the first time transform runs.
        raise RuntimeError("worker killed mid-transform")
    return {"stage": "transform", "rows": 100}


async def example_3_checkpoint_resume():
    print("\n=== Example 3: Checkpoint + resume ===")
    import tempfile

    persist_dir = tempfile.mkdtemp(prefix="tiny-workflow-")

    engine = WorkflowEngine(persist_dir=persist_dir)
    wf = engine.create("crash-recovery")
    wf.add_step("download", download_step)
    wf.add_step("transform", transform_step, deps=["download"])

    # First run crashes at transform.
    try:
        await engine.run("crash-recovery", input_data={"resume_ok": False})
    except WorkflowError as exc:
        print(f"Run 1 failed as expected: {exc}")

    # Restart in a fresh engine, re-declaring the same DAG. Engine.run()
    # auto-loads the checkpoint saved under the workflow name, so the
    # already-completed "download" step is skipped and only "transform" reruns.
    new_engine = WorkflowEngine(persist_dir=persist_dir)
    wf2 = new_engine.create("crash-recovery")
    wf2.add_step("download", download_step)
    wf2.add_step("transform", transform_step, deps=["download"])

    results = await new_engine.run("crash-recovery", input_data={"resume_ok": True})
    print(f"Run 2 resumed and completed: {results}")


async def main():
    await example_1_ai_agent_pipeline()
    await example_2_parallel_barrier()
    await example_3_checkpoint_resume()


if __name__ == "__main__":
    asyncio.run(main())
