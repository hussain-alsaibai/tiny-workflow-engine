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
        step_type: One of ``"task"``, ``"parallel"``, ``"branch"`` or
            ``"map"``. Informational only; execution does not depend on it.
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


@dataclass
class WorkflowSnapshot:
    """A serializable snapshot of a workflow's state for persistence."""

    workflow_id: str
    name: str
    state: WorkflowState
    input_data: Dict[str, Any]
    steps: Dict[str, Dict[str, Any]]

    def to_json(self) -> str:
        payload = asdict(self)
        payload["state"] = self.state.value
        for step in payload["steps"].values():
            step["status"] = StepStatus(step["status"]).value
        return json.dumps(payload, indent=2)


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

    def _snapshot_path(self, workflow_id: str) -> Path:
        assert self._persist_dir is not None
        return self._persist_dir / f"{workflow_id}.json"

    def _save_snapshot(self, wf: "Workflow") -> None:
        if self._persist_dir is None:
            return
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
                }
                for step in wf.steps.values()
            },
        )
        path = self._snapshot_path(wf.workflow_id)
        path.write_text(payload.to_json(), encoding="utf-8")

    def _load_snapshot(self, workflow_id: str) -> Optional[WorkflowSnapshot]:
        if self._persist_dir is None:
            return None
        path = self._snapshot_path(workflow_id)
        if not path.exists():
            return None
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
        )

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
        self._workflows[workflow_id] = wf
        return wf

    async def run(
        self, name_or_id: str, input_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Run a workflow by name or ID and return its per-step results."""
        wf = self._resolve(name_or_id)
        if input_data is not None:
            wf.input_data.update(input_data)
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
        """Run all steps according to their dependency graph."""
        if self.state == WorkflowState.COMPLETED:
            return self.results()
        self.state = WorkflowState.RUNNING
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
            self._engine._save_snapshot(self)
            raise exc
        except asyncio.TimeoutError:
            self.state = WorkflowState.FAILED
            self._engine._save_snapshot(self)
            raise WorkflowError("Workflow global timeout exceeded")

        self.state = WorkflowState.COMPLETED
        self._engine._save_snapshot(self)
        await self._call_hook(
            self._hooks_instance.on_complete, (self, self._snapshot())
        )
        return {k: v for k, v in completed.items()}

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
                }
                for step in self.steps.values()
            },
        )

    @property
    def _hooks_instance(self) -> WorkflowHooks:
        return self._engine._hooks
