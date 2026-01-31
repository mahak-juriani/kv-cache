"""
Async TCP Server Module (Task 3)

This module implements the asynchronous TCP server for KV-Cache.

Students must implement:
- handle_client(): Handle a single client connection
- start(): Start the server and accept connections

Key asyncio concepts needed:
- asyncio.start_server(): Create a TCP server
- StreamReader.readline(): Read a line from client
- StreamWriter.write() / drain(): Send data to client
- Proper connection cleanup with writer.close() / wait_closed()
"""
import hashlib
import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from typing import Optional

from ..cache.store import KVStore
from ..config.settings import settings
from ..protocol.commands import CommandType, Response
from ..protocol.parser import ProtocolParser

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KVServer:
    """
    Asynchronous TCP server for the KV-Cache service.

    This server handles multiple concurrent clients using asyncio.
    Each client connection is handled in a separate coroutine,
    allowing for high concurrency without threading.

    Features:
    - Non-blocking I/O with asyncio
    - Persistent connections (multiple commands per connection)
    - Graceful error handling and connection cleanup
    - Shared KVStore across all connections

    Usage:
        server = KVServer(host='0.0.0.0', port=7171)
        await server.start()  # Runs forever

    Attributes:
        host: Server bind address (e.g., '0.0.0.0')
        port: Server port number (e.g., 7171)
        store: The KVStore instance shared by all connections
        parser: The ProtocolParser for parsing commands
    """

    def __init__(
            self,
            host: str = None,
            port: int = None,
            store: KVStore = None,
    ):
        """
        Initialize the server.

        Args:
            host: Bind address (default from settings)
            port: Port number (default from settings)
            store: KVStore instance (creates new one if not provided)
        """
        self.host = host if host is not None else settings.HOST
        self.port = port if port is not None else settings.PORT
        self.store = store if store is not None else KVStore()
        self.parser = ProtocolParser()

        # Server state
        self._server: Optional[asyncio.Server] = None
        self._running = False
        self._connection_count = 0
        self._total_requests = 0


    def _get_shard_id(self, key: str) -> int:
        h = hashlib.sha256(key.encode()).hexdigest()
        return int(h, 16) % settings.NUM_SHARDS


    def _get_primary_node(self, shard_id: int) -> int:
        return settings.SHARD_TO_NODE[shard_id]


    def _is_local_shard(self, key: str) -> bool:
        shard_id = self._get_shard_id(key)
        return self._get_primary_node(shard_id) == settings.NODE_ID
    
    async def _forward_request(self, raw_command: str, node_id: int) -> Response:
        host, port = settings.CLUSTER_NODES[node_id]

        reader, writer = await asyncio.open_connection(host, port)

        writer.write((raw_command + "\n").encode())
        await writer.drain()

        data = await reader.readline()

        writer.write(b"QUIT\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

        response_str = data.decode().strip()
        return self.parser.parse_response_string(response_str)



    async def handle_client(self, reader: StreamReader, writer: StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        logger.info(f"Client connected: {addr}")
        self._connection_count += 1

        try:
            while True:
                data = await reader.readline()
                if not data:
                    break

                try:
                    message = data.decode().strip()
                except UnicodeDecodeError:
                    response = Response.error("invalid encoding")
                    writer.write(self.parser.format_response(response).encode())
                    await writer.drain()
                    continue

                # ---------- forwarding marker ----------
                is_forwarded = False
                if message.startswith("__FORWARDED__"):
                    is_forwarded = True
                    message = message.replace("__FORWARDED__ ", "", 1)

                # ---------- ALWAYS create command ----------
                command = self.parser.parse_request(message)
                command.is_forwarded = is_forwarded

                # ---------- NOW it is safe ----------
                if command.type == CommandType.QUIT:
                    logger.info(f"Client quit: {addr}")
                    break

                response = await self._execute_command(command)
                writer.write(self.parser.format_response(response).encode())
                await writer.drain()

        except ConnectionResetError:
            logger.warning(f"Client reset connection: {addr}")
        except Exception as e:
            logger.exception(f"Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Client disconnected: {addr}")


    async def _execute_command(self, command) -> Response:
        self._total_requests += 1

        # Commands without keys are always local
        if not command.key:
            return self._execute_local(command)

        # Shard check
        if not self._is_local_shard(command.key):
            shard_id = self._get_shard_id(command.key)
            node_id = self._get_primary_node(shard_id)
            return await self._forward_request(command.raw, node_id)

        return self._execute_local(command)




    async def start(self) -> None:
        """
        Start the server and begin accepting connections.

        This method creates the asyncio server and runs forever
        (or until cancelled). It should be called from asyncio.run()
        or within an existing event loop.

        Implementation:
        - Use asyncio.start_server() with self.handle_client as callback
        - Log the server address when started
        - Use server.serve_forever() to run indefinitely

        Example:
            server = KVServer(port=7171)
            asyncio.run(server.start())
        """
        # === TODO START: Implement start ===
        self._server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )

        self._running = True
        addr = self._server.sockets[0].getsockname()
        logger.info(f"Server started on {addr}")

        async with self._server:
            await self._server.serve_forever()
        # === TODO END ===

    async def stop(self) -> None:
        """
        Stop the server gracefully.

        Closes the server and waits for it to fully shut down.
        """
        # === TODO START: Implement stop ===
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._running = False
            logger.info("Server stopped")
        # === TODO END ===

    def is_running(self) -> bool:
        """Check if the server is currently running."""
        return self._running

    def get_stats(self) -> dict:
        """
        Get server statistics.

        Returns:
            Dictionary with server stats including connection counts,
            request counts, and store statistics.
        """
        return {
            "running": self._running,
            "host": self.host,
            "port": self.port,
            "total_connections": self._connection_count,
            "total_requests": self._total_requests,
            "store_stats": self.store.get_stats(),
        }
    
    def _execute_local(self, command) -> Response:
        if command.type == CommandType.UNKNOWN:
            return Response.error("invalid command")

        if command.type == CommandType.PUT:
            self.store.put(command.key, command.value, command.ttl)
            return Response.stored()


        if command.type == CommandType.GET:
            value = self.store.get(command.key)
            if value is None:
                return Response.key_not_found()
            return Response.value_response(value)

        if command.type == CommandType.DELETE:
            if self.store.delete(command.key):
                return Response.deleted()
            return Response.key_not_found()

        if command.type == CommandType.EXISTS:
            return Response.ok("1" if self.store.exists(command.key) else "0")

        return Response.error("invalid command")


async def run_server(host: str = None, port: int = None) -> None:
    """
    Convenience function to create and run the server.

    Args:
        host: Bind address (default from settings)
        port: Port number (default from settings)

    Usage:
        asyncio.run(run_server(port=7171))
    """
    server = KVServer(host=host, port=port)

    try:
        await server.start()
    except asyncio.CancelledError:
        logger.info("Server shutdown requested")
    finally:
        await server.stop()



