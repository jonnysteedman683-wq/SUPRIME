"""Decentralised task coordination — the swarm's collective work engine.

Any node can *submit* a task to the swarm. Any node that has registered a
*handler* for that task's kind can execute it. There is no central scheduler:
coordination emerges from the replicated :class:`~suprime.store.DistributedStore`.

The claim protocol
------------------
1. A submitted task is written to the store as ``PENDING``.
2. Every worker that can handle the task writes its own *claim* entry, tagged
   with the time it claimed. Claims never conflict because each node writes a
   distinct key, so they all survive replication.
3. Once claims have propagated, every node independently computes the same
   winner — the earliest claim, ties broken by node id. Only the winner runs
   the handler and writes the result back as ``DONE`` (or ``FAILED``).

Because the winner is a pure function of replicated state, the swarm reaches
the same decision everywhere without a coordinator. Execution is at-least-once:
before claims fully converge two nodes may briefly both believe they won, so
handlers should be idempotent.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from .store import DistributedStore

TASK_PREFIX = "__task__/"
CLAIM_PREFIX = "__claim__/"

Handler = Callable[["Task"], Union[Any, Awaitable[Any]]]


class TaskState(str, Enum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"


@dataclass
class Task:
    id: str
    kind: str
    args: Dict[str, Any]
    state: TaskState
    submitted_by: str
    owner: Optional[str] = None
    result: Any = None
    error: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        return cls(
            id=data["id"],
            kind=data["kind"],
            args=data.get("args", {}),
            state=TaskState(data["state"]),
            submitted_by=data["submitted_by"],
            owner=data.get("owner"),
            result=data.get("result"),
            error=data.get("error"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "args": self.args,
            "state": self.state.value,
            "submitted_by": self.submitted_by,
            "owner": self.owner,
            "result": self.result,
            "error": self.error,
        }


class TaskBoard:
    """Coordinates distributed task submission and execution over the store.

    Args:
        node_id: The owning node's identity.
        store: The replicated store used as the shared coordination substrate.
        clock: Injectable time source used for claim timestamps.
    """

    def __init__(
        self,
        node_id: str,
        store: DistributedStore,
        clock: Callable[[], float] = time.time,
        claim_grace_rounds: int = 3,
    ) -> None:
        self._node_id = node_id
        self._store = store
        self._clock = clock
        #: Coordination rounds a claim must age before the winner may execute,
        #: giving competing claims time to propagate so the winner is computed
        #: from a converged view. Should exceed the swarm's gossip diameter.
        self._claim_grace_rounds = claim_grace_rounds
        self._handlers: Dict[str, Handler] = {}
        self._executed: set[str] = set()
        self._claim_age: Dict[str, int] = {}

    # -- registration & submission -----------------------------------------

    def register_handler(self, kind: str, handler: Handler) -> None:
        """Declare that this node can execute tasks of ``kind``."""
        self._handlers[kind] = handler

    def submit(self, kind: str, args: Optional[Dict[str, Any]] = None, task_id: Optional[str] = None) -> str:
        """Publish a new task to the swarm and return its id."""
        import uuid

        tid = task_id or uuid.uuid4().hex
        task = Task(
            id=tid,
            kind=kind,
            args=args or {},
            state=TaskState.PENDING,
            submitted_by=self._node_id,
        )
        self._store.set(TASK_PREFIX + tid, task.to_dict())
        return tid

    # -- queries ------------------------------------------------------------

    def get_task(self, task_id: str) -> Optional[Task]:
        data = self._store.get(TASK_PREFIX + task_id)
        return Task.from_dict(data) if data else None

    def tasks(self) -> List[Task]:
        return [
            Task.from_dict(v)
            for k, v in self._store.items()
            if k.startswith(TASK_PREFIX)
        ]

    def _claims(self, task_id: str) -> Dict[str, float]:
        prefix = CLAIM_PREFIX + task_id + "/"
        claims: Dict[str, float] = {}
        for key, value in self._store.items():
            if key.startswith(prefix):
                claims[value["node_id"]] = value["claim_ts"]
        return claims

    def _winner(self, task_id: str) -> Optional[str]:
        claims = self._claims(task_id)
        if not claims:
            return None
        # Earliest claim wins; node id breaks ties deterministically.
        return min(claims.items(), key=lambda kv: (kv[1], kv[0]))[0]

    # -- the coordination step ---------------------------------------------

    async def evaluate(self) -> None:
        """Advance every pending task by one coordination step.

        Called on each swarm tick. Claims handleable pending tasks, then
        executes any task this node has deterministically won.
        """
        for task in self.tasks():
            if task.state is not TaskState.PENDING:
                self._claim_age.pop(task.id, None)
                continue
            if task.kind not in self._handlers:
                continue
            self._maybe_claim(task)
            # Let claims settle for a few rounds before anyone executes, so the
            # winner is chosen from a converged claim set (avoids duplicate runs).
            age = self._claim_age.get(task.id, 0) + 1
            self._claim_age[task.id] = age
            if age < self._claim_grace_rounds:
                continue
            await self._maybe_execute(task)

    def _maybe_claim(self, task: Task) -> None:
        claim_key = CLAIM_PREFIX + task.id + "/" + self._node_id
        if claim_key not in self._store:
            self._store.set(
                claim_key, {"node_id": self._node_id, "claim_ts": self._clock()}
            )

    async def _maybe_execute(self, task: Task) -> None:
        if task.id in self._executed:
            return
        if self._winner(task.id) != self._node_id:
            return
        self._executed.add(task.id)
        handler = self._handlers[task.kind]
        try:
            result = handler(task)
            if hasattr(result, "__await__"):
                result = await result  # type: ignore[assignment]
            task.state = TaskState.DONE
            task.result = result
            task.owner = self._node_id
        except Exception as exc:  # noqa: BLE001 - surface handler errors to swarm
            task.state = TaskState.FAILED
            task.error = repr(exc)
            task.owner = self._node_id
        self._store.set(TASK_PREFIX + task.id, task.to_dict())
