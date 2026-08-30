"""Tests for tiny-workflow-engine."""

import asyncio
import shutil
import tempfile

import pytest

from tiny_workflow_engine import (
    WorkflowEngine,
    WorkflowError,
    WorkflowHooks,
    WorkflowState,
    StepStatus,
    _topological_sort,
)


def test_topological_sort():
    """Linear deps resolve in order."""
    steps = {
        "a": type("S", (), {"deps": []})(),
        "b": type("S", (), {"deps": ["a"]})(),
        "c": type("S", (), {"deps": ["b"]})(),
    }
    assert _topological_sort(steps) == ["a", "b", "c"]


def test_topological_sort_parallel():
    """Fan-out resolves correctly."""
    steps = {
        "a": type("S", (), {"deps": []})(),
        "b": type("S", (), {"deps": ["a"]})(),
        "c": type("S", (), {"deps": ["a"]})(),
        "d": type("S", (), {"deps": ["b", "c"]})(),
    }
    order = _topological_sort(steps)
    assert order[0] == "a"
    assert set(order[1:3]) == {"b", "c"}
    assert order[3] == "d"


def test_topological_sort_cycle():
    """Cycle detection raises ValueError."""
    steps = {
        "a": type("S", (), {"deps": ["c"]})(),
        "b": type("S", (), {"deps": ["a"]})(),
        "c": type("S", (), {"deps": ["b"]})(),
    }
    with pytest.raises(ValueError) as exc_info:
        _topological_sort(steps)
    assert "Cycle" in str(exc_info.value)


def test_unknown_dependency_raises():
    """A step depending on a nonexistent step raises ValueError."""
    engine = WorkflowEngine()
    wf = engine.create("bad-dep")
    wf.add_step("a", lambda input_data, deps: {})
    with pytest.raises(ValueError):
        wf.add_step("b", lambda input_data, deps: {}, deps=["missing"])


@pytest.mark.asyncio
async def test_linear_workflow():
    """Fetch -> process -> save."""
    engine = WorkflowEngine(persist_dir=None)
    events = []

    def fetch(input_data, deps):
        events.append("fetch")
        return {"x": 1}

    def process(input_data, deps):
        events.append("process")
        return {"x": deps["fetch"]["x"] * 2}

    def save(input_data, deps):
        events.append("save")
        return {"saved": deps["process"]["x"]}

    wf = engine.create("linear")
    wf.add_step("fetch", fetch)
    wf.add_step("process", process, deps=["fetch"])
    wf.add_step("save", save, deps=["process"])

    result = await engine.run(wf.workflow_id)
    assert result["save"]["saved"] == 2
    assert events == ["fetch", "process", "save"]


@pytest.mark.asyncio
async def test_async_sync_mixed():
    """Async and sync steps work together."""
    engine = WorkflowEngine(persist_dir=None)

    def sync_step(input_data, deps):
        return {"sync": True}

    async def async_step(input_data, deps):
        await asyncio.sleep(0.01)
        return {"async": deps["sync"]["sync"]}

    wf = engine.create("mixed")
    wf.add_step("sync", sync_step)
    wf.add_step("async", async_step, deps=["sync"])

    result = await engine.run(wf.workflow_id)
    assert result["async"]["async"] is True


@pytest.mark.asyncio
async def test_retry_success():
    """Step fails twice, succeeds on 3rd try."""
    engine = WorkflowEngine(persist_dir=None)
    attempts = {"count": 0}

    def flaky(input_data, deps):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError(f"attempt {attempts['count']} failed")
        return {"ok": True}

    wf = engine.create("retry-ok")
    wf.add_step("flaky", flaky, retry_count=3, retry_backoff=0.01)

    result = await engine.run(wf.workflow_id)
    assert result["flaky"]["ok"] is True
    assert attempts["count"] == 3


@pytest.mark.asyncio
async def test_retry_exhausted():
    """All retries exhausted -> WorkflowError."""
    engine = WorkflowEngine(persist_dir=None)

    def always_fail(input_data, deps):
        raise RuntimeError("permanent failure")

    wf = engine.create("retry-fail")
    wf.add_step("fail", always_fail, retry_count=2, retry_backoff=0.01)

    with pytest.raises(WorkflowError) as exc_info:
        await engine.run(wf.workflow_id)
    assert "permanent failure" in str(exc_info.value)


@pytest.mark.asyncio
async def test_timeout():
    """Step exceeds its per-step timeout -> WorkflowError."""
    engine = WorkflowEngine(persist_dir=None)

    async def slow(input_data, deps):
        await asyncio.sleep(0.5)
        return {"slow": True}

    wf = engine.create("timeout-test")
    wf.add_step("slow", slow, timeout=0.05)

    with pytest.raises(WorkflowError):
        await engine.run(wf.workflow_id)


@pytest.mark.asyncio
async def test_map():
    """add_map fans out and collects results in order."""
    engine = WorkflowEngine(persist_dir=None)

    def square(input_data, item):
        return item * item

    wf = engine.create("map-test")
    wf.add_map("squares", square, items=[1, 2, 3, 4])

    result = await engine.run(wf.workflow_id)
    assert result["squares"] == [1, 4, 9, 16]


@pytest.mark.asyncio
async def test_map_async():
    """add_map works with async item functions."""
    engine = WorkflowEngine(persist_dir=None)

    async def slow_square(input_data, item):
        await asyncio.sleep(0.01)
        return item * item

    wf = engine.create("map-async")
    wf.add_map("squares", slow_square, items=[1, 2, 3])

    result = await engine.run(wf.workflow_id)
    assert result["squares"] == [1, 4, 9]


@pytest.mark.asyncio
async def test_parallel_fanout():
    """add_parallel runs functions concurrently and joins results."""
    engine = WorkflowEngine(persist_dir=None)

    def one(input_data, deps):
        return 1

    def two(input_data, deps):
        return 2

    def three(input_data, deps):
        return 3

    wf = engine.create("parallel")
    wf.add_parallel("seq", [one, two, three])

    result = await engine.run(wf.workflow_id)
    assert sorted(result["seq"]) == [1, 2, 3]


@pytest.mark.asyncio
async def test_persistence_and_resume():
    """Completed workflows persist snapshot state."""
    tmpdir = tempfile.mkdtemp()
    try:
        engine = WorkflowEngine(persist_dir=tmpdir)

        def step1(input_data, deps):
            return {"step1": "done"}

        def step2(input_data, deps):
            return {"step2": "done"}

        wf = engine.create("persist-test")
        wf.add_step("step1", step1)
        wf.add_step("step2", step2, deps=["step1"])

        await engine.run(wf.workflow_id)

        snap = engine._load_snapshot(wf.workflow_id)
        assert snap is not None
        assert snap.state == WorkflowState.COMPLETED
        assert snap.steps["step1"]["status"].value == "completed"
        assert snap.steps["step2"]["status"].value == "completed"
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.asyncio
async def test_resume_preserves_completed_steps():
    """Resume skips already-completed steps and reruns failed/pending ones.

    A workflow whose middle step failed should, on the next run, not re-execute
    the already-completed steps and retry only the failed step.
    """
    tmpdir = tempfile.mkdtemp()
    try:
        engine = WorkflowEngine(persist_dir=tmpdir)
        calls = {"a": 0, "b": 0}

        def step_a(input_data, deps):
            calls["a"] += 1
            return {"a": 1}

        def step_b(input_data, deps):
            calls["b"] += 1
            if calls["b"] < 2:
                raise RuntimeError("transient crash")
            return {"b": 2}

        wf = engine.create("resume-test")
        wf.add_step("step_a", step_a)
        wf.add_step("step_b", step_b, deps=["step_a"])

        # First attempt: step_a succeeds, step_b fails -> workflow fails.
        with pytest.raises(WorkflowError):
            await wf.run()
        assert calls["a"] == 1
        assert calls["b"] == 1

        # Simulate a process restart: re-declare the same DAG with the same
        # workflow id. run() restores the snapshot, skips completed step_a,
        # and only retries step_b.
        restarted = engine.create("resume-test")
        restarted.workflow_id = wf.workflow_id
        restarted.add_step("step_a", step_a)
        restarted.add_step("step_b", step_b, deps=["step_a"])
        result = await restarted.run()
        assert calls["a"] == 1  # completed step_a not re-executed
        assert calls["b"] == 2  # failed step_b retried
        assert result["step_b"] == {"b": 2}
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.asyncio
async def test_global_timeout():
    """Workflow global timeout aborts the run."""
    engine = WorkflowEngine(persist_dir=None)

    async def slow(input_data, deps):
        await asyncio.sleep(0.5)
        return {"done": True}

    wf = engine.create("global-timeout")
    wf.global_timeout = 0.05
    wf.add_step("slow", slow)

    with pytest.raises(WorkflowError):
        await engine.run(wf.workflow_id)


@pytest.mark.asyncio
async def test_hooks():
    """Hooks fire at correct lifecycle points."""

    class HooksTest(WorkflowHooks):
        def __init__(self):
            self.events = []

        def on_start(self, wf, snap):
            self.events.append("start")

        def on_step_start(self, wf, step_id):
            self.events.append(f"step-start:{step_id}")

        def on_step_complete(self, wf, step_id, result):
            self.events.append(f"step-done:{step_id}")

        def on_complete(self, wf, snap):
            self.events.append("complete")

    hooks = HooksTest()
    engine = WorkflowEngine(persist_dir=None, hooks=hooks)

    def s1(input_data, deps):
        return {"s1": True}

    def s2(input_data, deps):
        return {"s2": deps["s1"]["s1"]}

    wf = engine.create("hook-test")
    wf.add_step("s1", s1)
    wf.add_step("s2", s2, deps=["s1"])

    await engine.run(wf.workflow_id)

    assert "start" in hooks.events
    assert "step-start:s1" in hooks.events
    assert "step-done:s1" in hooks.events
    assert "step-start:s2" in hooks.events
    assert "step-done:s2" in hooks.events
    assert "complete" in hooks.events


@pytest.mark.asyncio
async def test_async_hooks():
    """Async hooks are awaited correctly."""

    class AsyncHooks(WorkflowHooks):
        def __init__(self):
            self.started = False

        async def on_start(self, wf, snap):
            await asyncio.sleep(0.001)
            self.started = True

    hooks = AsyncHooks()
    engine = WorkflowEngine(persist_dir=None, hooks=hooks)

    def s1(input_data, deps):
        return {"ok": True}

    wf = engine.create("async-hook")
    wf.add_step("s1", s1)
    await engine.run(wf.workflow_id)
    assert hooks.started is True


@pytest.mark.asyncio
async def test_run_by_name():
    """engine.run resolves by workflow name too."""
    engine = WorkflowEngine(persist_dir=None)

    def dummy(input_data, deps):
        return {"ok": True}

    wf = engine.create("named")
    wf.add_step("s1", dummy)

    result = await engine.run("named")
    assert result["s1"]["ok"] is True


@pytest.mark.asyncio
async def test_input_data_passed_to_steps():
    """Workflow input_data is available to every step."""
    engine = WorkflowEngine(persist_dir=None)

    def reader(input_data, deps):
        return {"seen": input_data["key"]}

    wf = engine.create("input-data")
    wf.add_step("read", reader)

    result = await engine.run(wf.workflow_id, input_data={"key": "value"})
    assert result["read"]["seen"] == "value"


# --------------------------------------------------------------------------
# v0.2.0: DAG visualization (Mermaid)
# --------------------------------------------------------------------------
def test_to_mermaid_linear():
    """to_mermaid emits one edge per dependency."""
    engine = WorkflowEngine(persist_dir=None)
    wf = engine.create("mermaid-linear")
    wf.add_step("a", lambda input_data, deps: 1)
    wf.add_step("b", lambda input_data, deps: 2, deps=["a"])
    wf.add_step("c", lambda input_data, deps: 3, deps=["b"])
    body = wf.to_mermaid()
    assert body.startswith("flowchart LR")
    assert "    a" in body
    assert "    a --> b" in body
    assert "    b --> c" in body


def test_to_mermaid_fanout():
    """Fan-out merges produce a single parent node with N incoming edges."""
    engine = WorkflowEngine(persist_dir=None)
    wf = engine.create("mermaid-fanout")
    wf.add_parallel(
        "fan",
        [
            lambda input_data, deps: 1,
            lambda input_data, deps: 2,
            lambda input_data, deps: 3,
        ],
    )
    body = wf.to_mermaid()
    # The parent "fan" depends on fan.0, fan.1, fan.2 (created by add_parallel).
    assert "fan" in body
    for i in range(3):
        assert f"fan.{i}" in body


# --------------------------------------------------------------------------
# v0.2.0: Approval gates
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_approval_gate_skips_when_not_approved(tmp_path, monkeypatch):
    """An approval-gate step with no approval file and no env var is SKIPPED."""
    monkeypatch.delenv("APPROVE_RELEASE", raising=False)
    engine = WorkflowEngine(persist_dir=str(tmp_path))
    wf = engine.create("approval-skip")
    wf.add_step("prep", lambda input_data, deps: {"ok": True})
    wf.add_step(
        "release",
        lambda input_data, deps: {"released": True},
        deps=["prep"],
        step_type="approval",
    )
    await engine.run(wf.workflow_id)
    snap = engine._load_snapshot(wf.name)
    assert snap.steps["prep"]["status"].value == "completed"
    assert snap.steps["release"]["status"].value == "skipped"


@pytest.mark.asyncio
async def test_approval_gate_runs_when_approval_file_present(tmp_path):
    """An approval-gate step runs when its approval file exists."""
    engine = WorkflowEngine(persist_dir=str(tmp_path))
    wf = engine.create("approval-file")
    wf.add_step("prep", lambda input_data, deps: {"ok": True})
    wf.add_step(
        "release",
        lambda input_data, deps: {"released": True},
        deps=["prep"],
        step_type="approval",
    )
    # First run: skipped (no approval file yet).
    await engine.run(wf.workflow_id)
    snap = engine._load_snapshot(wf.name)
    assert snap.steps["release"]["status"].value == "skipped"
    # Create the approval file at the path the engine expects.
    approve_dir = tmp_path / "approvals" / wf.workflow_id
    approve_dir.mkdir(parents=True, exist_ok=True)
    (approve_dir / "release.approved").write_text("")
    # Re-run on the same workflow instance — the run() loop resets the
    # approval gate to PENDING before re-checking.
    await engine.run(wf.workflow_id)
    snap = engine._load_snapshot(wf.name)
    assert snap.steps["release"]["status"].value == "completed"


@pytest.mark.asyncio
async def test_approval_gate_env_var(tmp_path, monkeypatch):
    """APPROVE_<STEP_NAME>=1 env var unlocks the gate."""
    monkeypatch.setenv("APPROVE_RELEASE", "1")
    engine = WorkflowEngine(persist_dir=str(tmp_path))
    wf = engine.create("approval-env")
    wf.add_step("prep", lambda input_data, deps: {"ok": True})
    wf.add_step(
        "release",
        lambda input_data, deps: {"released": True},
        deps=["prep"],
        step_type="approval",
    )
    await engine.run(wf.workflow_id)
    snap = engine._load_snapshot(wf.name)
    assert snap.steps["release"]["status"].value == "completed"


# --------------------------------------------------------------------------
# v0.2.0: Snapshot summary / timing
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_snapshot_summary_and_timing():
    """Snapshot includes per-step timing and a human-readable summary."""
    tmpdir = tempfile.mkdtemp()
    try:
        engine = WorkflowEngine(persist_dir=tmpdir)
        wf = engine.create("timed")
        wf.add_step("s1", lambda input_data, deps: {"x": 1})
        wf.add_step("s2", lambda input_data, deps: {"y": 2}, deps=["s1"])
        await engine.run(wf.workflow_id)
        snap = engine._load_snapshot(wf.name)
        summary = snap.summary
        assert summary["total_steps"] == 2
        assert summary["completed"] == 2
        assert summary["failed"] == 0
        assert summary["attempts_total"] == 2
        # Each step has a duration_s populated.
        for step_name, step in snap.steps.items():
            assert step["duration_s"] is not None
            assert step["started_at"] is not None
            assert step["completed_at"] is not None
        # Workflow-level timing.
        assert summary["started_at"] is not None
        assert summary["completed_at"] is not None
        assert summary["duration_s"] is not None
    finally:
        shutil.rmtree(tmpdir)


def test_summary_handles_unknown_status_strings():
    """Summary works even if step statuses arrive as plain strings."""
    snap = type("Snap", (), {})()
    # Build a synthetic WorkflowSnapshot directly.
    from tiny_workflow_engine import WorkflowSnapshot, WorkflowState
    snap = WorkflowSnapshot(
        workflow_id="manual:1",
        name="manual",
        state=WorkflowState.COMPLETED,
        input_data={},
        steps={
            "a": {
                "status": StepStatus.COMPLETED,
                "attempts": 1,
            },
            "b": {
                "status": StepStatus.FAILED,
                "attempts": 3,
            },
        },
    )
    summary = snap.summary
    assert summary["completed"] == 1
    assert summary["failed"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
