"""tiny-workflow-engine CLI — run workflows from the command line.

Install the package and the ``twf`` console script becomes available. You
can also invoke this module directly via ``python cli.py``.

Usage examples
--------------

List all persisted workflows::

    twf --list --persist-dir /tmp/workflows

Show the status of a single workflow::

    twf --status my-pipeline --persist-dir /tmp/workflows

Approve a pending approval-gate step (creates an approval file)::

    twf --approve my-pipeline/deploy-prod --persist-dir /tmp/workflows

Render a Mermaid flowchart for a workflow (the workflow must be loaded in a
running process; this CLI only inspects persisted snapshots, so for ``--dot``
we build a minimal in-memory workflow from the snapshot's step metadata)::

    twf --dot my-pipeline --persist-dir /tmp/workflows
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Optional


async def _cmd_list(persist_dir: str) -> int:
    pd = Path(persist_dir)
    if not pd.exists():
        print(f"(no persist dir: {pd})")
        return 0
    found = False
    for f in sorted(pd.glob("*.json")):
        print(f.stem)
        found = True
    if not found:
        print("(no persisted workflows)")
    return 0


async def _cmd_status(persist_dir: str, name: str) -> int:
    # Lazy import so the CLI works even if the engine is uninstalled.
    from tiny_workflow_engine import WorkflowEngine

    engine = WorkflowEngine(persist_dir=persist_dir)
    snapshot = engine._load_snapshot(name)
    if snapshot is None:
        print(f"No workflow found: {name}")
        return 1
    print(f"Workflow: {snapshot.workflow_id}")
    print(f"State:    {snapshot.state.value}")
    summary = snapshot.summary
    print(
        f"Steps:    {summary['completed']}/{summary['total_steps']} completed, "
        f"{summary['failed']} failed, {summary['pending']} pending"
    )
    if summary.get("duration_s") is not None:
        print(f"Duration: {summary['duration_s']:.4f}s")
    for name_, step in snapshot.steps.items():
        status = step["status"]
        status_str = status.value if hasattr(status, "value") else status
        attempts = step.get("attempts", 0)
        dur = step.get("duration_s")
        dur_str = f" ({dur:.4f}s)" if dur is not None else ""
        print(f"  {name_}: {status_str} ({attempts} attempts){dur_str}")
    return 0


async def _cmd_approve(persist_dir: str, target: str) -> int:
    """Create an approval file for a workflow/step pair.

    The engine's :meth:`Workflow._approval_gate` looks up approval files at
    ``<persist_dir>/approvals/<workflow_id>/<step>.approved``, where
    ``workflow_id`` is the ``name:sequence`` identifier used in
    :attr:`Workflow.workflow_id`. To make the CLI work without forcing the
    user to pass a sequence number, we look the workflow up in the persisted
    snapshot (which stores ``workflow_id``), and create the approval file
    under that exact id. If no snapshot exists yet, we fall back to the
    workflow name — the engine will then match the approval on the next run
    if and only if the new workflow_id happens to be the literal name (rare,
    but supported).
    """
    from tiny_workflow_engine import WorkflowEngine

    pd = Path(persist_dir)
    pd.mkdir(parents=True, exist_ok=True)
    approval_dir = pd / "approvals"
    approval_dir.mkdir(exist_ok=True)

    # ``--approve`` argument format: ``workflow_name/step_name``.
    parts = target.split("/", 1)
    if len(parts) != 2:
        print(
            "ERROR: --approve expects 'workflow_name/step_name', "
            f"got: {target!r}"
        )
        return 2
    wf_name, step_name = parts
    safe_step = step_name.replace("/", "_").replace("\\", "_")

    # Prefer the persisted snapshot's workflow_id so we match the path the
    # engine will check at run-time.
    engine = WorkflowEngine(persist_dir=persist_dir)
    snapshot = engine._load_snapshot(wf_name)
    if snapshot is not None and snapshot.workflow_id:
        approval_key = snapshot.workflow_id
    else:
        approval_key = wf_name
    safe_key = approval_key.replace("/", "_").replace("\\", "_")

    approve_file = approval_dir / safe_key / f"{safe_step}.approved"
    approve_file.parent.mkdir(parents=True, exist_ok=True)
    approve_file.write_text("", encoding="utf-8")
    print(f"Approved: {wf_name}/{step_name} ({approve_file})")
    return 0


async def _cmd_dot(persist_dir: str, name: str) -> int:
    """Render a Mermaid ``flowchart`` for the named workflow.

    This rebuilds the DAG purely from the persisted snapshot's step
    metadata (deps + status). The original step functions are not
    recovered — that's the user's responsibility when resuming — but
    the dependency graph itself is fully described by the snapshot.
    """
    from tiny_workflow_engine import WorkflowEngine

    engine = WorkflowEngine(persist_dir=persist_dir)
    snapshot = engine._load_snapshot(name)
    if snapshot is None:
        print(f"No workflow found: {name}")
        return 1

    lines = ["flowchart LR"]
    for step_name, step in snapshot.steps.items():
        deps = step.get("deps") or []
        if deps:
            edges = " --> ".join(deps)
            lines.append(f"    {edges} --> {step_name}")
        else:
            lines.append(f"    {step_name}")
    print("\n".join(lines))
    return 0


async def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(
        prog="twf",
        description="tiny-workflow-engine CLI",
    )
    p.add_argument(
        "--persist-dir",
        default="/tmp/workflows",
        help="Directory where workflow snapshots are stored (default: /tmp/workflows)",
    )
    p.add_argument(
        "--list",
        action="store_true",
        help="List persisted workflows",
    )
    p.add_argument(
        "--status",
        metavar="NAME",
        help="Show status of a workflow by name",
    )
    p.add_argument(
        "--approve",
        metavar="WORKFLOW/STEP",
        help="Approve a pending approval-gate step (format: workflow_name/step_name)",
    )
    p.add_argument(
        "--dot",
        metavar="NAME",
        help="Output a Mermaid flowchart for a workflow name",
    )

    args = p.parse_args(argv)

    if args.list:
        return await _cmd_list(args.persist_dir)
    if args.status:
        return await _cmd_status(args.persist_dir, args.status)
    if args.approve:
        return await _cmd_approve(args.persist_dir, args.approve)
    if args.dot:
        return await _cmd_dot(args.persist_dir, args.dot)

    p.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
