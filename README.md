# Endpoint-Scaffolding
Mini python frameworck builded on top of websockets to abstract most of the protocols behind classes like Client and Server

# EndpointScaffolding

EndpointScaffolding is a small framework to exchange messages between a Server and one or more Clients over an encrypted websocket connection. Every message has a type (a string) and a payload (a dictionary). You declare what should happen when a message of a given type arrives, and the framework takes care of the rest.

# Concepts

- Message type: a string that identifies a message, for example "greet".
- Payload: a dictionary with the data of the message.
- Receptor: an async function that runs when a message of a given type arrives.
- Middleware: an async function that runs before the receptor of the same type and decides if the message is accepted.
- Connection id: a string that identifies the other side of the connection. On a Server it is the id of the client that sent the message.

There are three ways to communicate:

- send: delivers a message and does not wait for anything.
- request: delivers a message and waits for the other side to answer.
- answer: replies to a message that was received through request.

# Importing

```python
from EndpointScaffolding import Server, Client
```

The exceptions are also available from the same module: ActionAssignationError, CommunicationError, FileError and SecurityError.

# Server

```python
Server(host="localhost", port=8765)
```

Creates a server. It does not start listening until it is called.

## Running the server

```python
await server()
```

Starts listening and keeps serving until the task is cancelled. To run it in a script use:

```python
asyncio.run(server())
```

## server.receptor(message_type)

Decorator that registers an async function as the receptor of a message type.

```python
@server.receptor("greet")
async def on_greet (payload, connection_id):
    ...
```

- payload: the dictionary sent by the client.
- connection_id: the id of the client that sent the message.
- Each message type can have only one receptor. Declaring it twice raises ActionAssignationError.
- The function must be async. Otherwise TypeError is raised.

## server.middleware(message_type)

Decorator that registers an async function that runs before the receptor of the same type.

```python
@server.middleware("greet")
async def check_name (payload, connection_id):
    return payload, "name" in payload
```

- It must return two values: the payload and a boolean.
- If the boolean is True the receptor runs with the returned payload.
- If the boolean is False the message is discarded and the receptor does not run. A client that used request will not receive an answer and will get a CommunicationError when the timeout expires.
- The returned payload can be a modified copy of the original one.
- Each message type can have only one middleware.

## await server.send(client_id, message_type, payload)

Sends a message to one client. It does not wait for an answer.

## await server.broadcast(message_type, payload)

Sends a message to every connected client.

## await server.request(client_id, message_type, payload, timeout=30.0)

Sends a message to one client and waits for its answer. It returns the answer message as a dictionary, and the data sent by the client is under the key "payload". If the client does not answer before the timeout (in seconds) a CommunicationError is raised.

## await server.answer(payload, return_data)

Replies to a message that was received by a receptor or a middleware. It must be called with the payload that the receptor received, and return_data is the dictionary that will be delivered to the other side. If the received message was not sent with request, a CommunicationError is raised.

# Client

```python
Client(host="localhost", port=8765)
```

Creates a client. It does not connect until it is called.

## Connecting and closing

There are two ways to connect. The first one is a context manager that closes the connection by itself:

```python
async with Client("localhost", 8765) as client:
    ...
```

The second one is manual:

```python
await client()
...
await client.close()
```

The first time a client connects to a server, it remembers the identity of that server. If the identity changes later, the connection is rejected with a SecurityError.

## client.receptor(message_type) and client.middleware(message_type)

They work exactly like the Server ones. The connection id that they receive is the id of the server connection.

## await client.send(message_type, payload)

Sends a message to the server. It does not wait for an answer.

## await client.request(message_type, payload, timeout=30.0)

Sends a message to the server and waits for its answer. It returns the answer message as a dictionary, and the data sent by the server is under the key "payload". If the server does not answer before the timeout a CommunicationError is raised.

## await client.answer(payload, return_data)

Replies to a message that the server sent with request. It must be called with the payload received by a receptor.

# Exceptions

- ActionAssignationError: a message type was declared more than once as a receptor or as a middleware.
- CommunicationError: a message could not be sent or received, the client is not connected, a request timed out or the connection was closed while waiting for an answer.
- FileError: the files used to store the security data could not be created, read or written.
- SecurityError: the connection is not encrypted or the identity of the server changed.

# Example with functional programming

Server, in a file called server.py:

```python
import asyncio
from EndpointScaffolding import Server

server = Server("localhost", 8765)

# Rejects greetings without a name here so the receptor can assume it exists
@server.middleware("greet")
async def check_name (payload, connection_id):
    return payload, "name" in payload

# Answers the request and then informs everyone, to show answer and broadcast
@server.receptor("greet")
async def on_greet (payload, connection_id):
    await server.answer(payload, {"message": f"Hello {payload['name']}"})
    await server.broadcast("notice", {"text": f"{payload['name']} joined"})

asyncio.run(server())
```

Client, in a file called client.py:

```python
import asyncio
from EndpointScaffolding import Client

client = Client("localhost", 8765)

# Shows the notices that the server broadcasts
@client.receptor("notice")
async def on_notice (payload, connection_id):
    print(payload["text"])

# The context manager guarantees that the connection is closed at the end
async def main ():
    async with client:
        answer = await client.request("greet", {"name": "Ana"})
        print(answer["payload"]["message"])
        await asyncio.sleep(1)

asyncio.run(main())
```

# Example with object oriented programming

Subclass Server or Client and register the methods as receptors in the constructor. The decorator is a normal method call, so it can receive a bound method.

Server, in a file called server.py:

```python
import asyncio
from EndpointScaffolding import Server

# Keeping the handlers as methods lets them share the state of the server
class GreeterServer(Server):

    def __init__ (self):
        super().__init__("localhost", 8765)
        self.middleware("greet")(self._check_name)
        self.receptor("greet")(self._on_greet)

    # Rejects greetings without a name so the receptor can assume it exists
    async def _check_name (self, payload, connection_id):
        return payload, "name" in payload

    # Answers the request and then informs everyone
    async def _on_greet (self, payload, connection_id):
        await self.answer(payload, {"message": f"Hello {payload['name']}"})
        await self.broadcast("notice", {"text": f"{payload['name']} joined"})

asyncio.run(GreeterServer()())
```

Client, in a file called client.py:

```python
import asyncio
from EndpointScaffolding import Client

# The public methods hide the message types from the rest of the program
class GreeterClient(Client):

    def __init__ (self):
        super().__init__("localhost", 8765)
        self.receptor("notice")(self._on_notice)

    # Shows the notices that the server broadcasts
    async def _on_notice (self, payload, connection_id):
        print(payload["text"])

    # Wraps the request so the caller only deals with a name and a message
    async def greet (self, name):
        answer = await self.request("greet", {"name": name})
        return answer["payload"]["message"]

async def main ():
    async with GreeterClient() as client:
        print(await client.greet("Ana"))
        await asyncio.sleep(1)

asyncio.run(main())
```

Start server.py first and then client.py. The client prints the greeting sent by the server followed by the notice that the server broadcasts.
