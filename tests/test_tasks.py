from suprime.tasks import Task, TaskState

def test_task_from_dict():
    data = {
        "fn": "dummy",
        "args": [1, 2],
        "kwargs": {"foo": "bar"},
        "id": "task-1",
        "result": "success",
        "error": None,
        "state": "pending",
        "worker": "node-1",
        "created": 123.0,
        "completed": 124.0,
        "kind": "compute",
        "submitted_by": "node-1"
    }

    task = Task.from_dict(data)

    assert task.id == "task-1"
    assert task.state == TaskState.PENDING
    assert task.result == "success"
    assert task.error is None

    # Validate specific fields depending on the Task schema version
    if hasattr(task, "fn"):
        assert task.fn == "dummy"
    if hasattr(task, "worker"):
        assert task.worker == "node-1"
    if hasattr(task, "created"):
        assert task.created == 123.0
    if hasattr(task, "completed"):
        assert task.completed == 124.0
    if hasattr(task, "kwargs"):
        assert task.kwargs == {"foo": "bar"}

def test_task_from_dict_defaults():
    data = {
        "fn": "dummy",
        "args": [],
        "kwargs": {},
        "id": "task-2",
        "state": "done",
        "created": 123.0,
        "kind": "fetch",
        "submitted_by": "node-2"
    }

    task = Task.from_dict(data)

    assert task.id == "task-2"
    assert task.state == TaskState.DONE
    assert task.result is None
    assert task.error is None

    if hasattr(task, "worker"):
        assert task.worker is None
    if hasattr(task, "completed"):
        assert task.completed is None
