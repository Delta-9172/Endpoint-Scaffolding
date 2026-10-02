from typing import Any, Dict, Tuple, Optional, Callable, Set
from abc import ABC, abstractmethod
import asyncio, websockets, json
import random
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

class ActionAssignationError(Exception):
    pass

class CommunicationError(Exception):
    pass

class FileError(Exception):
    pass

class SecurityError(Exception):
    pass

class _Incoming(dict):
    def __init__ (self, data, connection, request_id):
        super().__init__(data)
        self._connection = connection
        self._request_id = request_id

class _Connection:
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
    def get_id (self) -> str:
        return self._id_
    def _spawn (self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
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
    async def _shutdown (self) -> None:
        for future in list(self._active_requests.values()):
            if not future.done():
                future.set_exception(CommunicationError("connection closed"))
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    @staticmethod
    def _serialize (message: Dict) -> str:
        return json.dumps(message)
    @staticmethod
    def _deserialize (message: str) -> Dict:
        return json.loads(message)
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
    async def _run_middleware (self, message, payload, middlewares) -> Tuple[Dict, bool]:
        middleware = middlewares.get(message["type"])
        if middleware is None:
            return payload, True
        return await middleware(payload, self._id_)
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
    async def _dispatch_strategy (self, message, receptors, middlewares) -> None:
        self._spawn(self._dispatch(message, receptors, middlewares))
    async def _has_answer_strategy (self, message, receptors, middlewares) -> None:
        future = self._active_requests.get(message["id"])
        if future is not None and not future.done():
            future.set_result(message)
    async def receive_loop (self, receptors, middlewares) -> None:
        try:
            while True:
                await self._receive(receptors, middlewares)
        finally:
            await self._shutdown()
    async def send (self, message_type: str, payload: Dict[str, Any]) -> None:
        try:
            message = {"type": message_type, "payload": payload, "class": ""}
            await self._websocket.send(self._serialize(message))
        except websockets.exceptions.ConnectionClosed as e:
            raise CommunicationError("messaging was unsuccessful") from e
    def _get_request_id (self) -> str:
        characters = string.ascii_letters + string.digits
        while True:
            id_ = "".join(secrets.SystemRandom().choices(characters, k=24))
            if id_ not in self._active_requests:
                return id_
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
    async def close (self) -> None:
        await self._websocket.close()

class _Endpoint(ABC):
    def __init__ (self):
        self._receptors: Dict[str, Callable] = {}
        self._middlewares: Dict[str, Callable] = {}
    def receptor (self, message_type: str):
        def wrapper (func):
            if not inspect.iscoroutinefunction(func):
                raise TypeError("function or method must be defined asynchronously")
            if message_type in self._receptors:
                raise ActionAssignationError(f"the message type '{message_type}' can't be declared more than once")
            self._receptors[message_type] = func
            return func
        return wrapper
    def middleware (self, message_type: str):
        def wrapper (func):
            if not inspect.iscoroutinefunction(func):
                raise TypeError("function or method must be defined asynchronously")
            if message_type in self._middlewares:
                raise ActionAssignationError(f"the message type '{message_type}' can't be declared more than once")
            self._middlewares[message_type] = func
            return func
        return wrapper
    def validate_payload (self, payload: Dict[str, Any], structure: Dict[str, type]) -> bool:
        if set(payload.keys()) != set(structure.keys()):
            return False
        for key, value in payload.items():
            data_type = structure[key]
            if not isinstance(value, data_type):
                return False
        return True
    @abstractmethod
    def send (self):
        pass
    @abstractmethod
    def request (self):
        pass
    @abstractmethod
    def answer (self):
        pass

class _SecurityManager:
    _APP_DIR = ".endpoint_scaffolding"
    @classmethod
    def _data_dir (cls) -> Path:
        path = Path.home() / cls._APP_DIR
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as e:
            raise FileError(f"could not create security directory: {e}") from e
        return path
    @staticmethod
    def _atomic_write (path: Path, data: bytes, private: bool = False) -> None:
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        tmp.write_bytes(data)
        if private:
            try:
                os.chmod(tmp, 0o600)
            except OSError:
                pass
        os.replace(tmp, path)
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
    @classmethod
    def server_context (cls) -> ssl.SSLContext:
        certfile, keyfile = cls._server_identity()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(certfile=str(certfile), keyfile=str(keyfile))
        return context
    @staticmethod
    def client_context () -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
    @classmethod
    def _load_pins (cls, pinfile: Path) -> Dict[str, str]:
        try:
            return json.loads(pinfile.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        except (ValueError, OSError) as e:
            raise FileError(f"could not read pinned servers: {e}") from e
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
            pins[key] = fingerprint
            cls._atomic_write(pinfile, json.dumps(pins, indent=2).encode("utf-8"))
        elif not hmac.compare_digest(known, fingerprint):
            raise SecurityError(f"identity of server {key} changed")

class Server(_Endpoint):
    def __init__ (self, host: str = "localhost", port: int = 8765):
        super().__init__()
        self._host: str = host
        self._port: int = port
        self._clients: Dict[str, _Connection] = {}
    def _get_client_id (self) -> str:
        characters = string.ascii_letters + string.digits
        while True:
            id_ = "".join(secrets.SystemRandom().choices(characters, k=24))
            if id_ not in self._clients:
                return id_
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
    async def send (self, client_id: str, message_type: str, payload: Dict[str, Any]) -> None:
        await self._clients[client_id].send(message_type, payload)
    async def broadcast (self, message_type: str, payload: Dict[str, Any]) -> None:
        for id_ in list(self._clients.keys()):
            await self.send(id_, message_type, payload)
    async def request (
        self,
        client_id: str,
        message_type: str,
        payload: Dict[str, Any],
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        return await self._clients[client_id].request(message_type, payload, timeout)
    async def answer (self, payload, return_data: Dict[str, Any]) -> None:
        connection = getattr(payload, "_connection", None)
        if connection is None:
            raise CommunicationError("message was not received by this endpoint")
        await connection.answer(payload, return_data)
    async def __call__ (self) -> None:
        ssl_context = _SecurityManager.server_context()
        async with websockets.serve(self._handle_client, self._host, self._port, ssl=ssl_context):
            await asyncio.Future()
    def get_clients (self) -> Tuple[str]:
        return (client.get_id() for client in self._clients.values())

class Client(_Endpoint):
    def __init__ (
        self,
        host: str = "localhost",
        port: int = 8765,
        reconnect: bool = True,
        reconnect_delay: float = 1.0,
        reconnect_max_delay: float = 30.0,
        max_reconnect_attempts: Optional[int] = None,
    ):
        super().__init__()
        if reconnect_delay <= 0 or reconnect_max_delay < reconnect_delay:
            raise ValueError("reconnection delays are invalid")
        self._host: str = host
        self._port: int = port
        self._reconnect: bool = reconnect
        self._reconnect_delay: float = reconnect_delay
        self._reconnect_max_delay: float = reconnect_max_delay
        self._max_reconnect_attempts: Optional[int] = max_reconnect_attempts
        self._server_connection: Optional[_Connection] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._closing: bool = False
    def _get_server_id (self) -> str:
        characters = string.ascii_letters + string.digits
        return "".join(secrets.SystemRandom().choices(characters, k=24))
    async def send (self, message_type: str, payload: Dict[str, Any]) -> None:
        if self._server_connection is None:
            raise CommunicationError("client is not connected")
        await self._server_connection.send(message_type, payload)
    async def request (self, message_type: str, payload: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
        if self._server_connection is None:
            raise CommunicationError("client is not connected")
        return await self._server_connection.request(message_type, payload, timeout)
    async def answer (self, payload, return_data: Dict[str, Any]) -> None:
        connection = getattr(payload, "_connection", None)
        if connection is None or connection is not self._server_connection:
            raise CommunicationError("message was not received on the current connection")
        await connection.answer(payload, return_data)
    async def _open_connection (self) -> _Connection:
        uri = f"wss://{self._host}:{self._port}"
        try:
            websocket = await websockets.connect(uri, ssl=_SecurityManager.client_context())
        except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as e:
            raise CommunicationError(f"could not connect to {uri}") from e
        try:
            _SecurityManager.verify_server(websocket, self._host, self._port)
        except BaseException:
            await websocket.close()
            raise
        return _Connection(websocket, self._get_server_id())
    async def _reconnect_with_backoff (self) -> Optional[_Connection]:
        delay = self._reconnect_delay
        attempt = 0
        while self._max_reconnect_attempts is None or attempt < self._max_reconnect_attempts:
            attempt += 1
            await asyncio.sleep(random.uniform(delay / 2, delay))
            if self._closing:
                return None
            try:
                connection = await self._open_connection()
            except CommunicationError as e:
                _logger.warning(
                    "reconnection attempt %d to %s:%d failed: %s",
                    attempt, self._host, self._port, e,
                )
                delay = min(delay * 2, self._reconnect_max_delay)
                continue
            except (SecurityError, FileError):
                _logger.error("reconnection aborted: server could not be trusted", exc_info=True)
                return None
            if self._closing:
                await connection.close()
                return None
            return connection
        _logger.error("giving up reconnecting after %d attempts", attempt)
        return None
    async def _supervise (self, connection: _Connection) -> None:
        while True:
            try:
                await connection.receive_loop(self._receptors, self._middlewares)
            except CommunicationError:
                pass
            finally:
                if self._server_connection is connection:
                    self._server_connection = None
            if self._closing or not self._reconnect:
                return
            _logger.warning("connection to %s:%d lost, reconnecting", self._host, self._port)
            connection = await self._reconnect_with_backoff()
            if connection is None:
                return
            self._server_connection = connection
            _logger.info("reconnected to %s:%d", self._host, self._port)
    async def __call__ (self) -> None:
        if self._receive_task is not None and not self._receive_task.done():
            raise CommunicationError("client is already running")
        self._closing = False
        connection = await self._open_connection()
        self._server_connection = connection
        self._receive_task = asyncio.create_task(self._supervise(connection))
    async def close (self) -> None:
        self._closing = True
        connection, self._server_connection = self._server_connection, None
        task, self._receive_task = self._receive_task, None
        try:
            if connection is not None:
                await connection.close()
        finally:
            if task is not None:
                task.cancel()
                (result,) = await asyncio.gather(task, return_exceptions=True)
                if isinstance(result, Exception) and not isinstance(result, CommunicationError):
                    _logger.error("receive loop failed", exc_info=result)
    async def __aenter__ (self) -> "Client":
        await self()
        return self
    async def __aexit__ (self, exc_type, exc, tb) -> None:
        await self.close()
