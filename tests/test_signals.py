from webants.libs.signals import Signal


def _recorder(calls, name):
    async def receiver(sender, **kwargs):
        calls.append(name)
        return name

    return receiver


async def test_equal_priority_keeps_connection_order():
    """P1-08: equal priorities must not compare handler objects."""
    signal = Signal("test_equal_priority")
    calls = []
    signal.connect(_recorder(calls, "first"), priority=0)
    signal.connect(_recorder(calls, "second"), priority=0)

    responses = await signal.send("sender")

    assert calls == ["first", "second"]
    assert [result for _, result in responses] == ["first", "second"]


async def test_higher_priority_runs_first():
    """P1-08: higher priority values run earlier."""
    signal = Signal("test_priority_order")
    calls = []
    signal.connect(_recorder(calls, "low"), priority=0)
    signal.connect(_recorder(calls, "high"), priority=10)

    await signal.send("sender")

    assert calls == ["high", "low"]


async def test_sender_none_runs_once():
    """P2-15: send(None) must not concatenate the None list with itself."""
    signal = Signal("test_sender_none")
    calls = []
    signal.connect(_recorder(calls, "global"))

    await signal.send(None)

    assert calls == ["global"]


async def test_specific_and_global_receivers_both_run():
    signal = Signal("test_specific_and_global")
    calls = []
    signal.connect(_recorder(calls, "specific"), sender="target")
    signal.connect(_recorder(calls, "global"))

    await signal.send("target")

    assert calls == ["specific", "global"]

    await signal.send("other")
    assert calls == ["specific", "global", "global"]


async def test_duplicate_explicit_registrations_run_twice():
    signal = Signal("test_duplicate_registration")
    calls = []

    async def receiver(sender, **kwargs):
        calls.append("called")

    signal.connect(receiver, sender="target")
    signal.connect(receiver)

    await signal.send("target")

    assert calls == ["called", "called"]


async def test_disabled_receiver_is_skipped():
    signal = Signal("test_disabled")
    calls = []
    receiver = _recorder(calls, "receiver")
    signal.connect(receiver)

    with signal.disable(receiver):
        await signal.send("sender")

    assert calls == []


async def test_disconnect_removes_receiver():
    signal = Signal("test_disconnect")
    calls = []
    receiver = _recorder(calls, "receiver")
    signal.connect(receiver)
    signal.disconnect(receiver)

    await signal.send("sender")

    assert calls == []
