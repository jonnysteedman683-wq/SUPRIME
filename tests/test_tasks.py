import pytest
from suprime.tasks import Task, TaskState

def test_task_to_dict():
    task = Task(
        id="task_123",
        kind="test_job",
        args={"key": "value"},
        state=TaskState.PENDING,
        submitted_by="node_1",
        owner="node_2",
        result=42,
        error="none"
    )
    task_dict = task.to_dict()
    assert task_dict == {
        "id": "task_123",
        "kind": "test_job",
        "args": {"key": "value"},
        "state": "pending",
        "submitted_by": "node_1",
        "owner": "node_2",
        "result": 42,
        "error": "none"
    }

def test_task_to_dict_defaults():
    task = Task(
        id="task_123",
        kind="test_job",
        args={},
        state=TaskState.PENDING,
        submitted_by="node_1"
    )
    task_dict = task.to_dict()
    assert task_dict == {
        "id": "task_123",
        "kind": "test_job",
        "args": {},
        "state": "pending",
        "submitted_by": "node_1",
        "owner": None,
        "result": None,
        "error": None
    }
