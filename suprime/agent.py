"""The agent layer — turn a swarm into a collective AI.

SUPRIME provides the *coordination*; an :class:`Agent` gives each node a
**brain** (any callable) and wires it to the swarm's primitives so a fleet of
brains behaves as one resilient collective:

* **think** — a pluggable brain executes work items (a rule, a local model, or
  an LLM call — the interface is the same);
* **remember** — a shared *blackboard* (the replicated KV store) that every
  agent reads and writes and converges on;
* **communicate** — pub/sub announcements between agents;
* **decide together** — trust-weighted quorum voting for a collective answer,
  robust to a minority of wrong or adversarial agents.

The brain is deliberately just ``brain(task, agent) -> result`` (sync or async).
A deterministic function, a scikit model, or ``lambda t, a: call_llm(t.args)``
all satisfy it, so the intelligence is pluggable while the collective behaviour
— load-balancing, exactly-once execution, shared memory, consensus, failure
tolerance — comes from the swarm underneath.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from .kvstore import KVStore
from .tasks import Task

# A brain reacts to a work item with the agent as its context/tools.
Brain = Callable[["Task", "Agent"], Any]

QUESTION_TOPIC = "__agent_question__"


class Agent:
    """A single swarm node endowed with a brain and collective faculties.

    Args:
        node: The :class:`~suprime.node.SwarmNode` this agent runs on.
        brain: ``brain(task, agent) -> result`` (sync or async). Optional — an
            agent with no brain still participates in memory, comms and voting.
        kinds: Task kinds this agent's brain will handle.
        namespace: Prefix isolating this collective's shared memory.
    """

    def __init__(
        self,
        node,
        brain: Optional[Brain] = None,
        kinds: Tuple[str, ...] = ("task",),
        namespace: str = "agent/",
    ) -> None:
        self.node = node
        self.brain = brain
        self._kinds = tuple(kinds)
        #: Shared "blackboard" memory every agent in the collective converges on.
        self.blackboard = KVStore(node, namespace=namespace + "mem/")
        self._pubsub = None   # lazy: only wires a Plumtree when comms are used
        self._byz = None      # lazy: only gossips votes when voting is used
        self._answerer: Optional[Callable[[Any], Any]] = None
        if brain is not None:
            for kind in self._kinds:
                node.tasks.register_handler(kind, self._invoke_brain)

    @property
    def id(self) -> str:
        return self.node.id

    def _invoke_brain(self, task: "Task") -> Any:
        # TaskBoard awaits the result if the brain is async, so this supports
        # both sync and async brains transparently.
        return self.brain(task, self)  # type: ignore[misc]

    # -- distributed work --------------------------------------------------

    def assign(self, kind: str, **args: Any) -> str:
        """Submit a work item to the collective; a capable agent will run it."""
        return self.node.tasks.submit(kind, args)

    def result(self, task_id: str) -> Any:
        task = self.node.tasks.get_task(task_id)
        if task is None or task.state.value != "done":
            return None
        return task.result

    def task(self, task_id: str):
        return self.node.tasks.get_task(task_id)

    # -- shared memory (blackboard) ----------------------------------------

    def remember(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        self.blackboard.put(key, value, ttl=ttl)

    def recall(self, key: str, default: Any = None) -> Any:
        return self.blackboard.get(key, default)

    def memory(self) -> Dict[str, Any]:
        return dict(self.blackboard.scan())

    # -- communication -----------------------------------------------------

    def _ps(self):
        if self._pubsub is None:
            from .pubsub import PubSub

            self._pubsub = PubSub(self.node)
        return self._pubsub

    async def announce(self, topic: str, data: Any) -> str:
        """Broadcast ``data`` on ``topic`` to the whole collective."""
        return await self._ps().publish(topic, data)

    def listen(self, topic: str, callback: Callable[[str, Any], None]) -> None:
        self._ps().subscribe(topic, callback)

    # -- collective decision (quorum voting) -------------------------------

    def _byzantine(self):
        if self._byz is None:
            from .byzantine import ByzantineConsensus

            self._byz = ByzantineConsensus(self.node, quorum=0.5)
        return self._byz

    def vote(self, question: str, answer: Any) -> None:
        """Cast this agent's vote toward the collective answer for ``question``."""
        self._byzantine().vote(question, answer)

    def consensus(self, question: str) -> Any:
        """The collective's trust-weighted answer, or ``None`` if none yet."""
        return self._byzantine().accepted(question)

    def trust(self, agent_id: str) -> float:
        return self._byzantine().trust(agent_id)

    # -- ensemble Q&A (mixture-of-agents) ----------------------------------

    def answer_with(self, answerer: Callable[[Any], Any]) -> None:
        """Let this agent answer collective questions with ``answerer(question)``.

        On each question broadcast to the collective, the agent computes its own
        answer and votes it. The consensus across all agents is the collective's
        answer — an ensemble that outvotes wrong or adversarial members.
        """
        self._answerer = answerer
        self.listen(QUESTION_TOPIC, self._on_question)

    def _on_question(self, _topic: str, payload: Any) -> None:
        if self._answerer is None:
            return
        qid = payload["qid"]
        try:
            answer = self._answerer(payload["question"])
        except Exception:  # noqa: BLE001 - a broken answerer just abstains
            return
        self.vote(qid, answer)

    async def ask(self, question: Any) -> str:
        """Pose a question to the whole collective; returns its id.

        After the swarm settles, read the collective answer with
        :meth:`consensus` (using the returned id).
        """
        qid = uuid.uuid4().hex
        # The asker also answers, if it can, so it contributes to the ensemble.
        if self._answerer is not None:
            try:
                self.vote(qid, self._answerer(question))
            except Exception:  # noqa: BLE001
                pass
        await self.announce(QUESTION_TOPIC, {"qid": qid, "question": question})
        return qid


def build_agents(nodes: List[Any], brain: Optional[Brain] = None, **kwargs: Any) -> List[Agent]:
    """Convenience: wrap each node in :class:`Agent` sharing one brain/config."""
    return [Agent(n, brain=brain, **kwargs) for n in nodes]
