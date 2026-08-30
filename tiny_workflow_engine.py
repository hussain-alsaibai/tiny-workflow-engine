"""tiny-workflow-engine: a zero-dependency async workflow engine.

A minimal DAG-based workflow engine for Python that supports async and sync
steps, parallel fan-out, conditional branching, persistence with resume,
retry with exponential backoff, per-step and global timeouts, and lifecycle
hooks. Designed for orchestrating AI agent pipelines, cron job coordination,
and multi-step data processing — all in a single pure-Python file.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

logger = logging.getLogger("tiny-workflow-engine")


class WorkflowError(Exception):
    """Raised when a workflow or one of its steps cannot complete."""


class StepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


def _is_async(func: Callable[..., Any]) -> bool:
    """Return True if *func* is a coroutine function."""
    return inspect.iscoroutinefunction(func)


def _topological_sort(steps: Dict[str, "Step"]) -> List[str]:
    """Return the dependency-respecting execution order of *steps*.

    Raises ``ValueError`` if a dependency cycle is detected (the graph must
    be a DAG). Steps with no dependencies come first; independent steps may
    be ordered arbitrarily but their relative order is deterministic within
    a single run.
    """
    permanent: set = set()
    temporary: set = set()
    order: List[str] = []

    def visit(name: str) -> None:
        if name in permanent:
            return
        if name in temporary:
            raise ValueError(f"Cycle detected in workflow at step '{name}'")
        temporary.add(name)
        for dep in steps[name].deps:
            if dep not in steps:
                raise ValueError(
                    f"Step '{name}' depends on unknown step '{dep}'"
                )
            visit(dep)
        temporary.remove(name)
        permanent.add(name)
        order.append(name)

    for name in steps:
        visit(name)
    return order


@dataclass
class Step:
    """A unit of work inside a workflow.

    Attributes:
        name: Unique identifier of the step within the workflow.
        func: The callable to execute. It is invoked as
            ``func(input_data, deps)`` where ``input_data`` is the workflow's
            shared input payload and ``deps`` maps each dependency's name to
            its result.
        deps: Names of steps that must complete before this step runs.
        retry_count: Number of additional attempts after the first failure.
        retry_backoff: Base delay (in seconds) for exponential backoff
            between attempts. The wait before attempt ``n`` is
            ``retry_backoff * (1 << (n - 1))``.
        timeout: Maximum runtime in seconds for a single attempt. ``None``
            means no per-step timeout.
        step_type: One of ``"task"``, ``"parallel"``, ``"branch"``,
            ``"map"`` or ``"approval"``. Informational only, except for
            ``"approval"``: such steps are checked at run-time and only
            executed when an approval file or environment variable is present
            (see :meth:`Workflow._approval_gate`).
        result: The latest successful return value of ``func``.
        error: Message of the last failure, if any.
        attempts: Number of times ``func`` has been invoked.
    """

    name: str
    func: Callable[..., Any]
    deps: List[str] = field(default_factory=list)
    retry_count: int = 0
    retry_backoff: float = 1.0
    timeout: Optional[float] = None
    step_type: str = "task"
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    attempts: int = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    duration_s: Optional[float] = None


@dataclass
class WorkflowSnapshot:
    """A serializable snapshot of a workflow's state for persistence."""

    workflow_id: str
    name: str
    state: WorkflowState
    input_data: Dict[str, Any]
    steps: Dict[str, Dict[str, Any]]
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    duration_s: Optional[float] = None

    def to_json(self) -> str:
        payload = asdict(self)
        payload["state"] = self.state.value
        for step in payload["steps"].values():
            step["status"] = StepStatus(step["status"]).value
        return json.dumps(payload, indent=2)

    @property
    def summary(self) -> Dict[str, Any]:
        """Return a human-readable summary of the snapshot."""
        total = len(self.steps)
        by_status: Dict[str, int] = {}
        attempts_total = 0
        for step in self.steps.values():
            status = step["status"].value if hasattr(step["status"], "value") else step["status"]
            by_status[status] = by_status.get(status, 0) + 1
            attempts_total += int(step.get("attempts", 0) or 0)
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "state": self.state.value,
            "total_steps": total,
            "completed": by_status.get("completed", 0) + by_status.get("skipped", 0),
            "failed": by_status.get("failed", 0),
            "running": by_status.get("running", 0),
            "pending": by_status.get("pending", 0),
            "skipped": by_status.get("skipped", 0),
            "attempts_total": attempts_total,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_s": self.duration_s,
        }


class WorkflowHooks:
    """Lifecycle hooks callback interface.

    Subclass this to observe lifecycle events. Every method is optional and
    may be ``async def`` if you need to await inside a hook.
    """

    def on_start(self, wf: "Workflow", snapshot: WorkflowSnapshot) -> None:
        """Called when a workflow run begins."""

    def on_step_start(self, wf: "Workflow", step_id: str) -> None:
        """Called before a step begins its first (or next) attempt."""

    def on_step_complete(
        self, wf: "Workflow", step_id: str, result: Any
    ) -> None:
        """Called after a step completes successfully."""

    def on_step_fail(self, wf: "Workflow", step_id: str, error: str) -> None:
        """Called after all retries of a step are exhausted."""

    def on_complete(self, wf: "Workflow", snapshot: WorkflowSnapshot) -> None:
        """Called when the whole workflow completes successfully."""

    def on_fail(self, wf: "Workflow", snapshot: WorkflowSnapshot) -> None:
        """Called when the workflow finishes with an unrecoverable error."""


StateHook = Callable[..., Any]
StepType = TypeVar("StepType", bound=Step)


class WorkflowEngine:
    """Factory for creating and running workflows.

    Args:
        persist_dir: Directory where checkpoints are written. Pass ``None``
            to disable persistence.
        hooks: Optional ``WorkflowHooks`` subclass instance for lifecycle
            observation.
    """

    def __init__(
        self,
        persist_dir: Optional[Union[str, Path]] = None,
        hooks: Optional[WorkflowHooks] = None,
    ) -> None:
        self._persist_dir = Path(persist_dir) if persist_dir else None
        if self._persist_dir is not None:
            self._persist_dir.mkdir(parents=True, exist_ok=True)
        self._hooks = hooks or WorkflowHooks()
        self._workflows: Dict[str, "Workflow"] = {}

    def create(
        self, name: str, input_data: Optional[Dict[str, Any]] = None
    ) -> "Workflow":
        """Create a new (or reload an existing) workflow by *name*."""
        workflow_id = f"{name}:{self._next_sequence(name)}"
        wf = Workflow(
            workflow_id=workflow_id,
            name=name,
            engine=self,
            input_data=input_data or {},
        )
        self._workflows[workflow_id] = wf
        return wf

    def _next_sequence(self, name: str) -> int:
        base = f"{name}:"
        existing = [
            key for key in self._workflows if key.startswith(base)
        ]
        nums = [int(key.split(":", 1)[1]) for key in existing]
        suffix = _counter.get(name, 0)
        _counter[name] = suffix + 1
        return suffix + (max(nums) + 1 if nums else 1)

    def _snapshot_path(self, name: str) -> Path:
        assert self._persist_dir is not None
        safe = name.replace("/", "_").replace("\\", "_")
        return self._persist_dir / f"{safe}.json"

    def _save_snapshot(self, wf: "Workflow") -> None:
        if self._persist_dir is None:
            return
        now = time.time()
        payload = WorkflowSnapshot(
            workflow_id=wf.workflow_id,
            name=wf.name,
            state=wf.state,
            input_data=dict(wf.input_data),
            steps={
                step.name: {
                    "name": step.name,
                    "deps": list(step.deps),
                    "retry_count": step.retry_count,
                    "retry_backoff": step.retry_backoff,
                    "timeout": step.timeout,
                    "step_type": step.step_type,
                    "status": step.status.value,
                    "result": step.result,
                    "error": step.error,
                    "attempts": step.attempts,
                    "started_at": getattr(step, "started_at", None),
                    "completed_at": getattr(step, "completed_at", None),
                    "duration_s": getattr(step, "duration_s", None),
                }
                for step in wf.steps.values()
            },
            started_at=wf._started_at,
            completed_at=wf._completed_at,
            duration_s=(
                (wf._completed_at - wf._started_at)
                if (wf._started_at is not None and wf._completed_at is not None)
                else None
            ),
        )
        path = self._snapshot_path(wf.name)
        path.write_text(payload.to_json(), encoding="utf-8")

    def _load_snapshot(self, name_or_id: str) -> Optional[WorkflowSnapshot]:
        if self._persist_dir is None:
            return None
        # Normalize an instance id ("name:3") to its stable name ("name").
        base = name_or_id.split(":", 1)[0]
        candidates = [name_or_id, base]
        for key in candidates:
            path = self._snapshot_path(key)
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            data["state"] = WorkflowState(data["state"])
            for step in data["steps"].values():
                step["status"] = StepStatus(step["status"])
            return WorkflowSnapshot(
                workflow_id=data["workflow_id"],
                name=data["name"],
                state=data["state"],
                input_data=data["input_data"],
                steps=data["steps"],
                started_at=data.get("started_at"),
                completed_at=data.get("completed_at"),
                duration_s=data.get("duration_s"),
            )
        return None

    def resume(self, workflow_id: str) -> "Workflow":
        """Reload a previously persisted workflow for re-running."""
        snapshot = self._load_snapshot(workflow_id)
        if snapshot is None:
            raise WorkflowError(
                f"No snapshot found for workflow '{workflow_id}'"
            )
        wf = Workflow(
            workflow_id=snapshot.workflow_id,
            name=snapshot.name,
            engine=self,
            input_data=snapshot.input_data,
        )
        wf._restore(snapshot)
        self._workflows[snapshot.workflow_id] = wf
        return wf

    async def run(
        self, name_or_id: str, input_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Run a workflow by name or ID and return its per-step results.

        If a persisted checkpoint exists for the workflow name, the run
        resumes from that checkpoint (already-completed steps are skipped).
        """
        wf = self._resolve(name_or_id)
        if input_data is not None:
            wf.input_data.update(input_data)
        # Resume from a checkpoint keyed by the workflow's stable name.
        snapshot = self._load_snapshot(wf.name)
        if snapshot is not None and snapshot.workflow_id != wf.workflow_id:
            wf._restore(snapshot)
        return await wf.run()

    def _resolve(self, name_or_id: str) -> "Workflow":
        if name_or_id in self._workflows:
            return self._workflows[name_or_id]
        # Match the most recently created workflow by that name.
        ordered = [
            key for key in self._workflows if key.split(":", 1)[0] == name_or_id
        ]
        if not ordered:
            raise WorkflowError(f"Workflow '{name_or_id}' not found")
        return self._workflows[ordered[-1]]


_counter: Dict[str, int] = {}


class Workflow:
    """A DAG of named steps that runs to completion when executed."""

    def __init__(
        self,
        workflow_id: str,
        name: str,
        engine: WorkflowEngine,
        input_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.workflow_id = workflow_id
        self.name = name
        self._engine = engine
        self.steps: Dict[str, Step] = {}
        self.input_data: Dict[str, Any] = input_data or {}
        self.state = WorkflowState.PENDING
        self.global_timeout: Optional[float] = None
        self._started_at: Optional[float] = None
        self._completed_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    def add_step(
        self,
        name: str,
        func: Callable[..., Any],
        deps: Optional[Sequence[str]] = None,
        retry_count: int = 0,
        retry_backoff: float = 1.0,
        timeout: Optional[float] = None,
        step_type: str = "task",
    ) -> "Workflow":
        """Add a task step with optional dependencies/retry/timeout."""
        if name in self.steps:
            raise ValueError(f"Step '{name}' already exists")
        self.steps[name] = Step(
            name=name,
            func=func,
            deps=list(deps or []),
            retry_count=retry_count,
            retry_backoff=retry_backoff,
            timeout=timeout,
            step_type=step_type,
        )
        self._validate()
        return self

    def add_parallel(
        self,
        name: str,
        funcs: Sequence[Callable[..., Any]],
        deps: Optional[Sequence[str]] = None,
        retry_count: int = 0,
        timeout: Optional[float] = None,
    ) -> "Workflow":
        """Fan out a list of functions concurrently into a single step.

        Each function is invoked with ``func(input_data, item)``. The
        parent step's result is the list of every function's return value in
        the same order as *funcs*.
        """
        parent = Step(
            name=name,
            func=lambda input_data, deps: None,  # placeholder
            deps=list(deps or []),
            retry_count=retry_count,
            timeout=timeout,
            step_type="parallel",
        )
        if name in self.steps:
            raise ValueError(f"Step '{name}' already exists")
        self.steps[name] = parent

        children = []
        for idx, func in enumerate(funcs):
            child_name = f"{name}.{idx}"
            self.steps[child_name] = Step(
                name=child_name,
                func=func,
                deps=list(deps or []),
                retry_count=retry_count,
                timeout=timeout,
                step_type="parallel",
            )
            children.append(child_name)
        parent.deps = list(deps or []) + children

        async def gather(input_data: Any, deps: Dict[str, Any]) -> List[Any]:
            return [deps[child] for child in children]

        parent.func = gather
        self._validate()
        return self

    def add_map(
        self,
        name: str,
        item_func: Callable[..., Any],
        items: Sequence[Any],
        deps: Optional[Sequence[str]] = None,
        retry_count: int = 0,
        timeout: Optional[float] = None,
    ) -> "Workflow":
        """Apply *item_func* to each item in *items* concurrently.

        ``item_func`` receives ``(input_data, item)``. The step's result is a
        list of outputs in the same order as *items*.
        """
        async def map_runner(input_data: Any, deps: Dict[str, Any]) -> List[Any]:
            coros = []
            for item in items:
                if _is_async(item_func):
                    coros.append(item_func(input_data, item))
                else:
                    coros.append(asyncio.to_thread(item_func, input_data, item))
            return list(await asyncio.gather(*coros))

        return self.add_step(
            name, map_runner, deps=deps, retry_count=retry_count,
            timeout=timeout, step_type="map",
        )

    # ------------------------------------------------------------------
    # Validation & persistence
    # ------------------------------------------------------------------
    def _validate(self) -> None:
        _topological_sort(self.steps)

    def _restore(self, snapshot: WorkflowSnapshot) -> None:
        """Restore step meta-state from a snapshot, dropping stale steps."""
        self.state = snapshot.state
        for step in self.steps.values():
            meta = snapshot.steps.get(step.name)
            if meta is None:
                continue
            step.status = meta["status"]
            step.result = meta["result"]
            step.error = meta["error"]
            step.attempts = meta["attempts"]

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    async def run(self) -> Dict[str, Any]:
        """Run all steps according to their dependency graph.

        If a persisted snapshot exists for this workflow, completed steps are
        restored and skipped so the run resumes from the last checkpoint.

        Steps whose ``step_type`` is ``"approval"`` are only executed when an
        approval file or matching environment variable is present; otherwise
        they are recorded as :attr:`StepStatus.SKIPPED` and downstream
        steps still receive their result slot (set to ``None``).
        """
        snapshot = self._engine._load_snapshot(self.workflow_id)
        if snapshot is not None:
            self._restore(snapshot)

        # If the workflow previously completed but contained approval-gate
        # steps that were skipped, allow the operator to re-trigger those
        # gates by simply re-running. We reset those steps to PENDING so
        # the gate check below can flip them to COMPLETED on this run.
        if self.state == WorkflowState.COMPLETED:
            has_skipped_approval = any(
                step.step_type == "approval" and step.status == StepStatus.SKIPPED
                for step in self.steps.values()
            )
            if not has_skipped_approval:
                return self.results()
            for step in self.steps.values():
                if step.step_type == "approval" and step.status == StepStatus.SKIPPED:
                    step.status = StepStatus.PENDING
                    step.result = None
                    step.error = None
            self.state = WorkflowState.PENDING
        self.state = WorkflowState.RUNNING
        if self._started_at is None:
            self._started_at = time.time()
        self._engine._save_snapshot(self)
        await self._call_hook(
            self._hooks_instance.on_start, (self, self._snapshot())
        )

        order = _topological_sort(self.steps)
        completed: Dict[str, Any] = {}
        failed: Optional[str] = None

        start = time.monotonic()

        try:
            for step_id in order:
                if self._cutoff_reached(start):
                    raise WorkflowError(
                        "Global timeout exceeded before step "
                        f"'{step_id}' could run"
                    )
                step = self.steps[step_id]
                if step.status == StepStatus.COMPLETED:
                    # Re-add to completed so resumed runs include it in result.
                    completed[step.name] = step.result
                    continue
                # Approval-gate steps are re-evaluated on every run: a prior
                # SKIPPED status is reset so the gate can flip to APPROVED
                # once the approval file or env var is supplied.
                if step.step_type == "approval":
                    step.status = StepStatus.PENDING
                    step.result = None
                    step.error = None
                if step.status == StepStatus.SKIPPED:
                    completed[step.name] = step.result
                    continue
                # Approval gate: skip when not approved.
                if not await self._approval_gate(step):
                    step.status = StepStatus.SKIPPED
                    step.error = "not approved"
                    step.result = None
                    self._engine._save_snapshot(self)
                    completed[step.name] = None
                    continue
                await self._execute(step, completed)
                if step.status == StepStatus.COMPLETED:
                    completed[step.name] = step.result
                if step.status == StepStatus.FAILED:
                    failed = step.name
                    self.state = WorkflowState.FAILED
                    self._engine._save_snapshot(self)
                    await self._call_hook(
                        self._hooks_instance.on_fail, (self, self._snapshot())
                    )
                    raise WorkflowError(
                        f"Step '{step.name}' failed: {step.error}"
                    )
            if self._cutoff_reached(start):
                raise WorkflowError("Workflow global timeout exceeded")
        except WorkflowError as exc:
            if self.state != WorkflowState.FAILED:
                self.state = WorkflowState.FAILED
            self._completed_at = time.time()
            self._engine._save_snapshot(self)
            raise exc
        except asyncio.TimeoutError:
            self.state = WorkflowState.FAILED
            self._completed_at = time.time()
            self._engine._save_snapshot(self)
            raise WorkflowError("Workflow global timeout exceeded")

        self.state = WorkflowState.COMPLETED
        self._completed_at = time.time()
        self._engine._save_snapshot(self)
        await self._call_hook(
            self._hooks_instance.on_complete, (self, self._snapshot())
        )
        return {k: v for k, v in completed.items()}

    async def _approval_gate(self, step: Step) -> bool:
        """Check if a step is approved.

        For non-approval steps this returns ``True`` unconditionally. For
        ``step.step_type == "approval"`` steps the gate checks for the
        existence of an approval file under the engine's ``persist_dir`` and
        falls back to a matching ``APPROVE_<STEP_NAME>`` environment
        variable. Use :mod:`cli` (``twf --approve workflow/step``) to
        create the approval file.
        """
        if step.step_type != "approval":
            return True
        if self._engine._persist_dir is None:
            return False
        approval_path = (
            Path(self._engine._persist_dir)
            / "approvals"
            / self.workflow_id.replace("/", "_")
            / f"{step.name}.approved"
        )
        env_key = f"APPROVE_{step.name.upper()}"
        if approval_path.exists():
            return True
        return os.environ.get(env_key) == "1"

    def _cutoff_reached(self, start: float) -> bool:
        if self.global_timeout is None:
            return False
        return (time.monotonic() - start) >= self.global_timeout

    async def _execute(
        self,
        step: Step,
        completed: Dict[str, Any],
    ) -> None:
        await self._call_hook(
            self._hooks_instance.on_step_start, (self, step.name)
        )
        step.started_at = time.time()
        for attempt in range(step.retry_count + 1):
            step.attempts = attempt + 1
            step.status = StepStatus.RUNNING
            self._engine._save_snapshot(self)
            try:
                deps = {
                    dep: self.steps[dep].result
                    for dep in step.deps
                }
                if _is_async(step.func):
                    coro = step.func(self.input_data, deps)
                else:
                    coro = asyncio.to_thread(
                        step.func, self.input_data, deps
                    )
                if step.timeout is not None:
                    result = await asyncio.wait_for(
                        coro, timeout=step.timeout
                    )
                else:
                    result = await coro
                step.result = result
                step.status = StepStatus.COMPLETED
                step.error = None
                step.completed_at = time.time()
                step.duration_s = step.completed_at - (step.started_at or step.completed_at)
                self._engine._save_snapshot(self)
                await self._call_hook(
                    self._hooks_instance.on_step_complete,
                    (self, step.name, result),
                )
                return
            except Exception as exc:  # noqa: BLE001 - propagate as step error
                step.error = str(exc)
                if attempt < step.retry_count:
                    delay = step.retry_backoff * (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                step.status = StepStatus.FAILED
                step.completed_at = time.time()
                step.duration_s = step.completed_at - (step.started_at or step.completed_at)
                self._engine._save_snapshot(self)
                await self._call_hook(
                    self._hooks_instance.on_step_fail,
                    (self, step.name, str(exc)),
                )
                raise WorkflowError(
                    f"Step '{step.name}' failed after "
                    f"{step.retry_count + 1} attempt(s): {exc}"
                ) from exc

    async def _call_hook(self, hook, args) -> None:
        """Invoke a lifecycle hook, supporting both sync and async hooks."""
        result = hook(*args)
        if inspect.isawaitable(result):
            await result

    def results(self) -> Dict[str, Any]:
        """Return the results of every completed step keyed by step name."""
        return {
            step.name: step.result
            for step in self.steps.values()
            if step.status == StepStatus.COMPLETED
        }

    def to_mermaid(self) -> str:
        """Return a Mermaid flowchart diagram of the workflow DAG.

        Useful for rendering dependency graphs inline in Markdown, PR
        descriptions, or any Mermaid-aware viewer. The output uses
        ``flowchart LR`` (left-to-right) and emits one ``A --> B`` line per
        dependency edge, so fan-in nodes appear as multiple separate edges
        instead of an ambiguous chain.

        Example:
            >>> wf = Workflow(...)
            >>> print(wf.to_mermaid())
            flowchart LR
                fetch
                fetch --> process
                process --> save
        """
        lines = ["flowchart LR"]
        seen_nodes: set = set()
        # First pass: declare nodes that have no incoming edges.
        for step in self.steps.values():
            if not step.deps:
                lines.append(f"    {step.name}")
                seen_nodes.add(step.name)
        # Second pass: emit one edge per dependency.
        for step in self.steps.values():
            for dep in step.deps:
                lines.append(f"    {dep} --> {step.name}")
        return "\n".join(lines)

    def _snapshot(self) -> WorkflowSnapshot:
        return WorkflowSnapshot(
            workflow_id=self.workflow_id,
            name=self.name,
            state=self.state,
            input_data=dict(self.input_data),
            steps={
                step.name: {
                    "name": step.name,
                    "deps": list(step.deps),
                    "retry_count": step.retry_count,
                    "retry_backoff": step.retry_backoff,
                    "timeout": step.timeout,
                    "step_type": step.step_type,
                    "status": step.status.value,
                    "result": step.result,
                    "error": step.error,
                    "attempts": step.attempts,
                    "started_at": getattr(step, "started_at", None),
                    "completed_at": getattr(step, "completed_at", None),
                    "duration_s": getattr(step, "duration_s", None),
                }
                for step in self.steps.values()
            },
            started_at=self._started_at,
            completed_at=self._completed_at,
            duration_s=(
                (self._completed_at - self._started_at)
                if (self._started_at is not None and self._completed_at is not None)
                else None
            ),
        )

    @property
    def _hooks_instance(self) -> WorkflowHooks:
        return self._engine._hooks


# ---------------------------------------------------------------------------
# Console-script entry point: ``twf``.
#
# The CLI lives in a separate ``cli.py`` to keep ``tiny_workflow_engine.py``
# focused on the library API. We re-export it under the attribute name
# ``cli`` so that the ``[project.scripts]`` entry in ``pyproject.toml``
# (``twf = "tiny_workflow_engine:cli"``) can locate it.
# ---------------------------------------------------------------------------
try:  # pragma: no cover - import side effect
    from cli import main as cli  # type: ignore
except ImportError:  # pragma: no cover - the module may not be installed
    cli = None  # type: ignore[assignment]
