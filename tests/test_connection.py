from __future__ import annotations

import pytest

from wsproto.connection import CLIENT, SERVER, Connection, ConnectionState
from wsproto.events import (
    BytesMessage,
    CloseConnection,
    Ping,
    Pong,
    Request,
    TextMessage,
)
from wsproto.frame_protocol import CloseReason
from wsproto.utilities import LocalProtocolError


@pytest.mark.parametrize("client_sends", [True, False])
@pytest.mark.parametrize("final", [True, False])
def test_send_message(client_sends: bool, final: bool) -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)

    if client_sends:
        local = client
        remote = server
    else:
        local = server
        remote = client

    data = b"x" * 23
    remote.receive_data(local.send(BytesMessage(data=data, message_finished=final)))
    event = next(remote.events())
    assert isinstance(event, BytesMessage)
    assert event.data == data
    assert event.message_finished is final


@pytest.mark.parametrize("client_sends", [True, False])
@pytest.mark.parametrize(
    "code, reason",
    [(CloseReason.NORMAL_CLOSURE, "bye"), (CloseReason.GOING_AWAY, "👋👋")],
)
def test_closure(client_sends: bool, code: CloseReason, reason: str) -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)

    if client_sends:
        local = client
        remote = server
    else:
        local = server
        remote = client

    # Data sent before the close frame must be delivered before the
    # CloseConnection event.
    messages = ["m1", "m2", "m3"]
    payload = b""
    for data in messages:
        payload += local.send(TextMessage(data=data, message_finished=True))
    payload += local.send(CloseConnection(code=code, reason=reason))
    remote.receive_data(payload)

    for data in messages:
        event = next(remote.events())
        assert isinstance(event, TextMessage)
        assert event.data == data
        assert event.message_finished is True

    event = next(remote.events())
    assert isinstance(event, CloseConnection)
    assert event.code is code
    assert event.reason == reason

    assert remote.state is ConnectionState.REMOTE_CLOSING
    assert local.state is ConnectionState.LOCAL_CLOSING

    local.receive_data(remote.send(event.response()))
    event = next(local.events())
    assert isinstance(event, CloseConnection)
    assert event.code is code
    assert event.reason == reason

    assert remote.state is ConnectionState.CLOSED  # type: ignore[comparison-overlap]
    assert local.state is ConnectionState.CLOSED

    with pytest.raises(LocalProtocolError):
        local.receive_data(b"foobar")


def test_abnormal_closure() -> None:
    client = Connection(CLIENT)
    client.receive_data(None)
    event = next(client.events())
    assert isinstance(event, CloseConnection)
    assert event.code is CloseReason.ABNORMAL_CLOSURE
    assert client.state is ConnectionState.CLOSED


def test_closure_then_connection_drops() -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)

    # The peer sends its final messages immediately followed by a close
    # frame, and then the underlying transport connection is lost before
    # the events are retrieved.
    messages = ["m1", "m2", "m3"]
    payload = b""
    for data in messages:
        payload += client.send(TextMessage(data=data, message_finished=True))
    payload += client.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    server.receive_data(payload)
    server.receive_data(None)

    for data in messages:
        event = next(server.events())
        assert isinstance(event, TextMessage)
        assert event.data == data

    event = next(server.events())
    assert isinstance(event, CloseConnection)
    assert event.code is CloseReason.NORMAL_CLOSURE
    assert server.state is ConnectionState.REMOTE_CLOSING

    # The close reply can still be sent, and no spurious abnormal closure
    # event is generated afterwards.
    server.send(event.response())
    assert server.state is ConnectionState.CLOSED
    assert list(server.events()) == []


def test_connection_drops_with_pending_data() -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)

    # Data that has already been received must be delivered before the
    # abnormal closure event, without waiting for more data.
    messages = ["m1", "m2", "m3"]
    for data in messages:
        server.receive_data(client.send(TextMessage(data=data, message_finished=True)))
    server.receive_data(None)

    for data in messages:
        event = next(server.events())
        assert isinstance(event, TextMessage)
        assert event.data == data

    event = next(server.events())
    assert isinstance(event, CloseConnection)
    assert event.code is CloseReason.ABNORMAL_CLOSURE
    assert server.state is ConnectionState.CLOSED

    with pytest.raises(LocalProtocolError):
        server.receive_data(b"foobar")


def test_close_whilst_closing() -> None:
    client = Connection(CLIENT)
    client.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    with pytest.raises(LocalProtocolError):
        client.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))


def test_send_after_close() -> None:
    client = Connection(CLIENT)
    client.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    with pytest.raises(LocalProtocolError):
        client.send(TextMessage(data="", message_finished=True))


@pytest.mark.parametrize("client_sends", [True, False])
def test_ping_pong(client_sends: bool) -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)

    if client_sends:
        local = client
        remote = server
    else:
        local = server
        remote = client

    payload = b"x" * 23
    remote.receive_data(local.send(Ping(payload=payload)))
    event = next(remote.events())
    assert isinstance(event, Ping)
    assert event.payload == payload

    local.receive_data(remote.send(event.response()))
    event = next(local.events())
    assert isinstance(event, Pong)
    assert event.payload == payload


def test_unsolicited_pong() -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)

    payload = b"x" * 23
    server.receive_data(client.send(Pong(payload=payload)))
    event = next(server.events())
    assert isinstance(event, Pong)
    assert event.payload == payload


@pytest.mark.parametrize("split_message", [True, False])
def test_data(split_message: bool) -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)

    data = "ƒñö®∂😎"
    server.receive_data(
        client.send(TextMessage(data=data, message_finished=not split_message)),
    )
    event = next(server.events())
    assert isinstance(event, TextMessage)
    assert event.message_finished is not split_message


def test_frame_protocol_gets_fed_garbage() -> None:
    client = Connection(CLIENT)

    payload = b"x" * 23
    frame = b"\x09" + bytearray([len(payload)]) + payload

    client.receive_data(frame)
    event = next(client.events())
    assert isinstance(event, CloseConnection)
    assert event.code == CloseReason.PROTOCOL_ERROR


def test_send_invalid_event() -> None:
    client = Connection(CLIENT)
    with pytest.raises(LocalProtocolError):
        client.send(Request(target="/", host="wsproto"))


def test_receive_data_when_closed() -> None:
    client = Connection(CLIENT)
    client._state = ConnectionState.CLOSED
    with pytest.raises(LocalProtocolError):
        client.receive_data(b"something")
