from typing import Any, Dict, Tuple, Optional, Callable, Set
from abc import ABC, abstractmethod
import asyncio, websockets, json
import inspect, secrets, string
import logging
import os, ssl
import hashlib, hmac, datetime
from pathlib import Path
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

_logger = logging.getLogger(__name__)

# Raised when a message type is bound to more than one handler, so a
# misconfiguration fails at declaration time instead of silently overriding
# a previously registered handler.
class ActionAssignationError(Exception):
    pass

# Single error type for every transport or protocol failure, so callers handle
# one exception instead of library specific ones (for example from websockets).
class CommunicationError(Exception):
    pass

# Kept apart from CommunicationError so callers can tell a local filesystem
# problem from a network problem.
class FileError(Exception):
    pass

# Kept apart from CommunicationError so a failed identity check is never
# handled as a harmless dropped connection.
class SecurityError(Exception):
    pass

# A dict subclass so handlers receive a plain payload while the origin
# connection and request id travel with it, which lets answer() reply without
# the user having to pass them around.
class _Incoming(dict):

    # The connection and request id are stored privately so only answer() can
    # use them to validate that a reply is legitimate.
    def __init__ (self, data, connection, request_id):
        super().__init__(data)
        self._connection = connection
        self._request_id = request_id

# Owns everything that is per connection (pending requests, running tasks), so
# Server and Client share exactly the same messaging logic.
class _Connection:

    # The strategy table replaces an if/elif chain on the message class, so a
    # new class is supported by registering one entry. on_error lets the owner
    # decide how handler failures surface.
    def __init__ (
        self,
        websocket,
        id_: str,
        on_error: Optional[Callable[[BaseException], None]] = None,
    ):
        self._id_ = id_
        self._websocket = websocket
        self._on_error = on_error
        self._reception_strategies = {
            "": self._dispatch_strategy,
            "request": self._dispatch_strategy,
            "request_answer": self._has_answer_strategy,
        }
        self._active_requests: Dict[str, asyncio.Future] = {}
        self._tasks: Set[asyncio.Task] = set()

    # Handlers run as separate tasks so a slow receptor never blocks the
    # receive loop. The tasks are tracked so they can be cancelled on shutdown.
    def _spawn (self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    # Central place for handler failures: without it, a task exception would go
    # unnoticed until the task is garbage collected.
    def _task_done (self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is None:
            return
        if self._on_error is not None:
            self._on_error(error)
        else:
            _logger.error(
                "middleware or receptor failed on connection %s",
                self._id_,
                exc_info=error,
            )

    # Fails the pending requests and cancels the handlers when the connection
    # ends, so no caller waits for an answer that can never arrive.
    async def _shutdown (self) -> None:
        for future in list(self._active_requests.values()):
            if not future.done():
                future.set_exception(CommunicationError("connection closed"))
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # Serialization is isolated in one place so the wire format can change
    # without touching the messaging logic.
    @staticmethod
    def _serialize (message: Dict) -> str:
        return json.dumps(message)

    # Counterpart of _serialize, isolated for the same reason.
    @staticmethod
    def _deserialize (message: str) -> Dict:
        return json.loads(message)

    # Every parsing failure becomes a CommunicationError, so a malformed message
    # from a peer is reported as a protocol error instead of crashing the loop.
    async def _receive (self, receptors, middlewares) -> None:
        try:
            raw = await self._websocket.recv()
        except websockets.exceptions.ConnectionClosed as e:
            raise CommunicationError("reception was unsuccessful") from e
        try:
            message = self._deserialize(raw)
            message_class = message.get("class", "")
            strategy = self._reception_strategies[message_class]
            required_key = "id" if message_class == "request_answer" else "type"
            if required_key not in message:
                raise CommunicationError(required_key)
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            raise CommunicationError("malformed message received") from e
        await strategy(message, receptors, middlewares)

    # A message type without middleware is valid by default, which makes
    # middleware opt in.
    async def _run_middleware (self, message, payload, middlewares) -> Tuple[Dict, bool]:
        middleware = middlewares.get(message["type"])
        if middleware is None:
            return payload, True
        return await middleware(payload, self._id_)

    # The payload is wrapped again after the middleware because the middleware
    # may return a new dict that lost the connection and request id.
    async def _dispatch (self, message, receptors, middlewares) -> None:
        request_id = message.get("id") if message.get("class") == "request" else None
        payload = _Incoming(message["payload"], self, request_id)
        payload, is_valid = await self._run_middleware(message, payload, middlewares)
        if not is_valid:
            return
        payload = _Incoming(payload, self, request_id)
        receptor = receptors.get(message["type"])
        if receptor is not None:
            await receptor(payload, self._id_)

    # Spawned instead of awaited so a slow handler never delays the next
    # message.
    async def _dispatch_strategy (self, message, receptors, middlewares) -> None:
        self._spawn(self._dispatch(message, receptors, middlewares))

    # Resolves the waiting future instead of dispatching, because an answer
    # belongs to a caller blocked in request() and not to a receptor.
    async def _has_answer_strategy (self, message, receptors, middlewares) -> None:
        future = self._active_requests.get(message["id"])
        if future is not None and not future.done():
            future.set_result(message)

    # The finally block guarantees the shutdown runs however the loop ends
    # (closed connection, protocol error or cancellation).
    async def receive_loop (self, receptors, middlewares) -> None:
        try:
            while True:
                await self._receive(receptors, middlewares)
        finally:
            await self._shutdown()

    # Plain messages use the empty class, which the receiving side dispatches
    # exactly like a request that expects no answer.
    async def send (self, message_type: str, payload: Dict[str, Any]) -> None:
        try:
            message = {"type": message_type, "payload": payload, "class": ""}
            await self._websocket.send(self._serialize(message))
        except websockets.exceptions.ConnectionClosed as e:
            raise CommunicationError("messaging was unsuccessful") from e

    # Ids come from SystemRandom and are retried on collision, so a peer can
    # neither predict them nor clash with a pending request.
    def _get_request_id (self) -> str:
        characters = string.ascii_letters + string.digits
        while True:
            id_ = "".join(secrets.SystemRandom().choices(characters, k=24))
            if id_ not in self._active_requests:
                return id_

    # The future is registered before sending so an answer that arrives
    # immediately is never missed. The finally block always removes it, even on
    # timeout, to avoid leaking pending requests.
    async def request (
        self,
        message_type: str,
        payload: Dict[str, Any],
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        id_ = self._get_request_id()
        self._active_requests[id_] = asyncio.get_running_loop().create_future()
        try:
            message = {"type": message_type, "payload": payload, "class": "request", "id": id_}
            await self._websocket.send(self._serialize(message))
            return await asyncio.wait_for(self._active_requests[id_], timeout)
        except asyncio.TimeoutError as e:
            raise CommunicationError("request timed out") from e
        except websockets.exceptions.ConnectionClosed as e:
            raise CommunicationError("requesting was unsuccessful") from e
        finally:
            self._active_requests.pop(id_, None)

    # Ownership and request status are checked first so a reply can never be
    # sent through the wrong connection or for a message that expected none.
    async def answer (self, message, return_data: Dict[str, Any]) -> None:
        if not isinstance(message, _Incoming) or message._connection is not self:
            raise CommunicationError("message does not belong to this connection")
        if message._request_id is None:
            raise CommunicationError("message is not a request")
        try:
            response = {"type": "", "payload": return_data, "class": "request_answer", "id": message._request_id}
            await self._websocket.send(self._serialize(response))
        except websockets.exceptions.ConnectionClosed as e:
            raise CommunicationError("answering was unsuccessful") from e

    # Thin wrapper that keeps the websocket private to this class.
    async def close (self) -> None:
        await self._websocket.close()

# Shared base of Server and Client. Handler registration is identical on both
# sides, so it lives here, while send, request and answer are forced to be
# defined by each side.
class _Endpoint(ABC):

    # Handlers are stored by message type so dispatching is a single lookup.
    def __init__ (self):
        self._receptors: Dict[str, Callable] = {}
        self._middlewares: Dict[str, Callable] = {}

    # Decorator based registration keeps the declaration next to the handler
    # code. Decorated signature: (payload, connection id) -> None.
    def receptor (self, message_type: str):

        # Validation happens at declaration time, so mistakes fail on import
        # instead of on the first message.
        def wrapper (func):
            if not inspect.iscoroutinefunction(func):
                raise TypeError("function or method must be defined asynchronously")
            if message_type in self._receptors:
                raise ActionAssignationError(f"the message type '{message_type}' can't be declared more than once")
            self._receptors[message_type] = func
            return func
        return wrapper

    # Same registration mechanism as receptor. Decorated signature:
    # (payload, connection id) -> payload, is_valid.
    def middleware (self, message_type: str):

        # Same declaration time validation as the receptor wrapper.
        def wrapper (func):
            if not inspect.iscoroutinefunction(func):
                raise TypeError("function or method must be defined asynchronously")
            if message_type in self._middlewares:
                raise ActionAssignationError(f"the message type '{message_type}' can't be declared more than once")
            self._middlewares[message_type] = func
            return func
        return wrapper

    # The signatures differ between Server (needs a client id) and Client, so
    # the base class only declares that the method must exist.
    @abstractmethod
    def send (self):
        pass

    # Abstract for the same reason as send.
    @abstractmethod
    def request (self):
        pass

    # Abstract for the same reason as send.
    @abstractmethod
    def answer (self):
        pass

# Groups all TLS and certificate pinning logic in one place so the transport
# code stays free of security details. It is never instantiated: everything
# is a class method or a static method.
class _SecurityManager:
    _APP_DIR = ".endpoint_scaffolding"

    # The directory is created with mode 0o700 so private keys and pinned
    # fingerprints are not readable by other users.
    @classmethod
    def _data_dir (cls) -> Path:
        path = Path.home() / cls._APP_DIR
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as e:
            raise FileError(f"could not create security directory: {e}") from e
        return path

    # Writes to a temporary file and then replaces the target, so a crash never
    # leaves a half written key or pin file behind.
    @staticmethod
    def _atomic_write (path: Path, data: bytes, private: bool = False) -> None:
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        tmp.write_bytes(data)
        if private:
            try:
                os.chmod(tmp, 0o600)  # On Windows this is almost a no-op, so ignoring the failure is harmless
            except OSError:
                pass
        os.replace(tmp, path)

    # The certificate is self-signed and generated on first use so the framework
    # works without external setup. Trust comes from pinning instead of a CA.
    # The validity is very long because there is no renewal mechanism.
    @classmethod
    def _server_identity (cls) -> Tuple[Path, Path]:
        directory = cls._data_dir()
        certfile = directory / "server_cert.pem"
        keyfile = directory / "server_key.pem"
        if certfile.exists() and keyfile.exists():
            return certfile, keyfile
        try:
            key = ec.generate_private_key(ec.SECP256R1())
            name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "endpoint-scaffolding")])
            now = datetime.datetime.now(datetime.timezone.utc)
            cert = (
                x509.CertificateBuilder()
                .subject_name(name)
                .issuer_name(name)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now - datetime.timedelta(days=1))
                .not_valid_after(now + datetime.timedelta(days=36500))
                .sign(key, hashes.SHA256())
            )
            cls._atomic_write(
                keyfile,
                key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                ),
                private=True,
            )
            cls._atomic_write(certfile, cert.public_bytes(serialization.Encoding.PEM))
        except FileError:
            raise
        except Exception as e:
            raise FileError(f"could not generate server identity: {e}") from e
        return certfile, keyfile

    # TLS 1.2 is the minimum version to reject outdated protocols while staying
    # compatible with common clients.
    @classmethod
    def server_context (cls) -> ssl.SSLContext:
        certfile, keyfile = cls._server_identity()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(certfile=str(certfile), keyfile=str(keyfile))
        return context

    # Verification is disabled on purpose because the server certificate is
    # self-signed. The identity is checked afterwards by verify_server through
    # fingerprint pinning.
    @staticmethod
    def client_context () -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    # A missing file means no server has been trusted yet. Any other failure is
    # raised because ignoring it would silently discard the existing pins.
    @classmethod
    def _load_pins (cls, pinfile: Path) -> Dict[str, str]:
        try:
            return json.loads(pinfile.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (ValueError, OSError) as e:
            raise FileError(f"could not read pinned servers: {e}") from e

    # Trust on first use: the first fingerprint seen for a host and port is
    # stored, and any later mismatch is rejected. hmac.compare_digest is used
    # to avoid leaking information through comparison timing.
    @classmethod
    def verify_server (cls, websocket, host: str, port: int) -> None:
        ssl_object = websocket.transport.get_extra_info("ssl_object")
        if ssl_object is None:
            raise SecurityError("connection is not encrypted")
        der = ssl_object.getpeercert(binary_form=True)
        if not der:
            raise SecurityError("server did not present a certificate")
        fingerprint = hashlib.sha256(der).hexdigest()
        pinfile = cls._data_dir() / "known_servers.json"
        pins = cls._load_pins(pinfile)
        key = f"{host}:{port}"
        known = pins.get(key)
        if known is None:
            pins[key] = fingerprint  # First time: the server is trusted and remembered
            cls._atomic_write(pinfile, json.dumps(pins, indent=2).encode("utf-8"))
        elif not hmac.compare_digest(known, fingerprint):
            raise SecurityError(f"identity of server {key} changed")

# Accepts many clients and keeps them by id, so messages can be addressed to a
# specific client or broadcast to all of them.
class Server(_Endpoint):

    # Host and port default to localhost so a default server is never exposed
    # to the network by accident.
    def __init__ (self, host: str = "localhost", port: int = 8765):
        super().__init__()
        self._host: str = host
        self._port: int = port
        self._clients: Dict[str, _Connection] = {}

    # Ids are retried on collision because they are the only key used to
    # address a client.
    def _get_client_id (self) -> str:
        characters = string.ascii_letters + string.digits
        while True:
            id_ = "".join(secrets.SystemRandom().choices(characters, k=24))
            if id_ not in self._clients:
                return id_

    # A CommunicationError is the normal way a connection ends, so it is
    # swallowed here. The finally block always unregisters the client.
    async def _handle_client (self, websocket) -> None:
        id_ = self._get_client_id()
        client = _Connection(websocket, id_)
        self._clients[id_] = client
        try:
            await client.receive_loop(self._receptors, self._middlewares)
        except CommunicationError:
            pass
        finally:
            self._clients.pop(id_, None)

    # Delegates to the connection so the server holds no messaging logic of its
    # own.
    async def send (self, client_id: str, message_type: str, payload: Dict[str, Any]) -> None:
        await self._clients[client_id].send(message_type, payload)

    # Iterates over a copy of the ids because clients can connect or disconnect
    # while the broadcast is awaiting.
    async def broadcast (self, message_type: str, payload: Dict[str, Any]) -> None:
        for id_ in list(self._clients.keys()):
            await self.send(id_, message_type, payload)

    # Delegates to the connection for the same reason as send.
    async def request (
        self,
        client_id: str,
        message_type: str,
        payload: Dict[str, Any],
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        return await self._clients[client_id].request(message_type, payload, timeout)

    # The reply goes through the connection that received the payload, so the
    # user never has to say which client is being answered.
    async def answer (self, payload, return_data: Dict[str, Any]) -> None:
        connection = getattr(payload, "_connection", None)
        if connection is None:
            raise CommunicationError("message was not received by this endpoint")
        await connection.answer(payload, return_data)

    # Being callable makes the server runnable with asyncio.run(server()).
    # Awaiting an unresolved future keeps it serving until it is cancelled.
    async def __call__ (self) -> None:
        ssl_context = _SecurityManager.server_context()
        async with websockets.serve(self._handle_client, self._host, self._port, ssl=ssl_context):
            await asyncio.Future()

# Talks to a single server, so unlike Server it holds one connection and needs
# no client id in its messaging methods.
class Client(_Endpoint):

    # The connection stays empty until the client is called, which lets the
    # methods detect and report a client that is not connected.
    def __init__ (self, host: str = "localhost", port: int = 8765):
        super().__init__()
        self._host: str = host
        self._port: int = port
        self._server_connection: Optional[_Connection] = None
        self._receive_task: Optional[asyncio.Task] = None

    # There is only one connection, so no collision check is needed.
    def _get_server_id (self) -> str:
        characters = string.ascii_letters + string.digits
        return "".join(secrets.SystemRandom().choices(characters, k=24))

    # Fails with a clear error instead of an AttributeError when the client has
    # not connected yet.
    async def send (self, message_type: str, payload: Dict[str, Any]) -> None:
        if self._server_connection is None:
            raise CommunicationError("client is not connected")
        await self._server_connection.send(message_type, payload)

    # Same guard as send.
    async def request (self, message_type: str, payload: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
        if self._server_connection is None:
            raise CommunicationError("client is not connected")
        return await self._server_connection.request(message_type, payload, timeout)

    # The payload must come from the current connection, so an answer can never
    # be sent to a server that is no longer the active one.
    async def answer (self, payload, return_data: Dict[str, Any]) -> None:
        connection = getattr(payload, "_connection", None)
        if connection is None or connection is not self._server_connection:
            raise CommunicationError("message was not received on the current connection")
        await connection.answer(payload, return_data)

    # The server is verified before the connection is used and the socket is
    # closed on any failure (BaseException includes cancellation), so an
    # untrusted server never receives a message.
    async def __call__ (self) -> None:
        uri = f"wss://{self._host}:{self._port}"
        websocket = await websockets.connect(uri, ssl=_SecurityManager.client_context())
        try:
            _SecurityManager.verify_server(websocket, self._host, self._port)
        except BaseException:
            await websocket.close()
            raise
        self._server_connection = _Connection(websocket, self._get_server_id())
        self._receive_task = asyncio.create_task(
            self._server_connection.receive_loop(self._receptors, self._middlewares)
        )

    # The connection and task are detached first so calls made during the close
    # already see a disconnected client. A CommunicationError from the receive
    # loop is expected on close, so only other failures are logged.
    async def close (self) -> None:
        connection, self._server_connection = self._server_connection, None
        task, self._receive_task = self._receive_task, None
        if connection is None:
            return
        try:
            await connection.close()
        finally:
            if task is not None:
                (result,) = await asyncio.gather(task, return_exceptions=True)
                if isinstance(result, Exception) and not isinstance(result, CommunicationError):
                    _logger.error("receive loop failed", exc_info=result)

    # Context manager support guarantees the connection is opened and closed as
    # a pair.
    async def __aenter__ (self) -> "Client":
        await self()
        return self

    # Always closes the connection, whether the block ended normally or with an
    # exception.
    async def __aexit__ (self, exc_type, exc, tb) -> None:
        await self.close()
