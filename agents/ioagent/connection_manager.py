from fastapi import WebSocket
import logging
import asyncio

# Configure logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        logger.info(f"📤 Sending to Client from connection manager: {message}")
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self.disconnect(websocket)
            raise

    async def broadcast(self, message: str):
        logger.info(f"📡 Broadcasting to {len(self.active_connections)} clients: {message}")
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Failed to broadcast to a client: {e}")
                disconnected.append(connection)
        # Clean up disconnected clients
        for conn in disconnected:
            self.disconnect(conn)

    async def wait_for_user_input(self, websocket: WebSocket = None):
        if not websocket:
            if not self.active_connections:
                raise Exception("No active connections to wait for input")
            websocket = self.active_connections[0]
        
        # Verify the websocket is still in our active connections
        if websocket not in self.active_connections:
            raise Exception("WebSocket is not in active connections")
        
        logger.info("Waiting for user input via WebSocket...")
        try:
            data = await websocket.receive_text()
            logger.info(f"Received user input: {data}")
            return data
        except Exception as e:
            logger.error(f"Error receiving from websocket: {e}")
            self.disconnect(websocket)
            raise Exception(f"WebSocket connection lost: {e}")

manager = ConnectionManager()
main_event_loop = None
