"""QQ channel implementation using OneBot protocol.

OneBot is a unified chatbot protocol standard that provides more flexibility
than the official QQ bot API. This implementation supports:

- Lagrange.onebot (recommended, actively maintained)
  - Project: https://github.com/LSTM-Kirigaya/Lagrange.onebot
  - Based on Lagrange.Core (NTQQ protocol implementation)
  - TypeScript library providing OneBot v11 interface
- go-cqhttp (legacy, may have compatibility issues)
- Shamrock (Android-based)
- Other OneBot v11 compatible adapters

## Lagrange.onebot vs go-cqhttp

**go-cqhttp** (已停止维护):
- 曾经最流行的 OneBot 适配器
- 已停止更新，可能存在兼容性问题
- QQ 客户端更新后可能无法正常工作

**Lagrange.onebot** (推荐):
- 基于 Lagrange.Core (NTQQ 协议的 C# 实现)
- 持续维护，兼容性更好
- 支持 Windows/macOS/Linux
- 功能完整：支持图片、视频、文件、转发等
- 提供 TypeScript SDK，易于集成
- 项目地址: https://github.com/LSTM-Kirigaya/Lagrange.onebot

## Setup

1. Install and configure Lagrange.onebot (see docs/Lagrange_OneBot_Setup.md)
2. Configure the adapter to expose HTTP API (default: http://localhost:5700)
3. Configure WebSocket reverse (recommended) or HTTP reverse for events
4. Update nanobot config with OneBot connection details

Example config.yaml:
  channels:
    qq:
      enabled: true
      api_url: "http://localhost:5700"
      access_token: ""  # Optional, if configured in adapter
      ws_reverse_url: "ws://localhost:8080/ws/reverse"  # Recommended
      bot_qq: 123456789  # Your bot's QQ number (for filtering self messages)
      allow_from: []  # Allowed QQ numbers (empty = public access)

## Advantages over official QQ API

- Can send active messages to groups (no 5-minute/5-reply limit)
- No monthly message quota restrictions
- More flexible message handling
- Support for more advanced features (file uploads, etc.)
"""

import asyncio
import json
from collections import deque
from typing import Any
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import QQConfig


class QQOneBotChannel(BaseChannel):
    """
    QQ channel using OneBot protocol.
    
    Compatible with OneBot v11 implementations like:
    - go-cqhttp (most popular)
    - NAPCAT
    - Shamrock (Android)
    - Other OneBot-compatible adapters
    
    Features:
    - HTTP API for sending messages
    - WebSocket reverse server for receiving events (NAPCAT connects to us)
    - Support for private messages and group messages
    - No official API limitations (can send active messages)
    """
    
    name = "qq"
    
    def __init__(self, config: QQConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: QQConfig = config
        self._session: aiohttp.ClientSession | None = None
        self._ws_clients: set[aiohttp.web.WebSocketResponse] = set()
        self._ws_server: web.Application | None = None
        self._ws_runner: web.AppRunner | None = None
        self._ws_site: web.TCPSite | None = None
        self._processed_ids: deque = deque(maxlen=1000)
        self._bot_qq: int | None = config.bot_qq
        
    async def start(self) -> None:
        """Start the OneBot channel."""
        if not self.config.api_url:
            logger.error("QQ OneBot API URL not configured")
            return
        
        self._running = True
        
        # Create HTTP session
        headers = {}
        if self.config.access_token:
            headers["Authorization"] = f"Bearer {self.config.access_token}"
        
        self._session = aiohttp.ClientSession(
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        )
        
        # Start WebSocket reverse server (NAPCAT connects to us)
        if self.config.ws_reverse_url:
            # Parse the URL to get host and port
            parsed = urlparse(self.config.ws_reverse_url)
            ws_host = parsed.hostname or "127.0.0.1"
            ws_port = parsed.port or 8080
            ws_path = parsed.path or "/ws/reverse"
            
            logger.info("Starting OneBot WebSocket reverse server on ws://{}:{}{}", ws_host, ws_port, ws_path)
            await self._start_websocket_server(ws_host, ws_port, ws_path)
        elif self.config.http_reverse_url:
            logger.info("Using OneBot HTTP reverse: {}", self.config.http_reverse_url)
            # HTTP reverse requires external webhook setup, so we'll use long polling
            await self._start_long_polling()
        else:
            logger.info("Using OneBot HTTP API only: {}", self.config.api_url)
            # No reverse connection, will only send messages (no incoming events)
            logger.warning(
                "No reverse connection configured. Bot can send messages but won't receive them. "
                "Configure ws_reverse_url to start WebSocket server for NAPCAT to connect."
            )
    
    async def _start_websocket_server(self, host: str, port: int, path: str) -> None:
        """Start WebSocket server for OneBot reverse connections (NAPCAT connects to us)."""
        app = web.Application()
        
        async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            
            self._ws_clients.add(ws)
            logger.info("OneBot WebSocket client connected from {}", request.remote)
            
            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            await self._handle_onebot_event(data)
                        except Exception as e:
                            logger.error("Error processing OneBot event: {}", e)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("WebSocket error: {}", ws.exception())
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        logger.info("WebSocket closed")
                        break
            finally:
                self._ws_clients.discard(ws)
                logger.info("OneBot WebSocket client disconnected")
            
            return ws
        
        app.router.add_get(path, websocket_handler)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        self._ws_server = app
        self._ws_runner = runner
        self._ws_site = site
        
        logger.info("OneBot WebSocket reverse server started on ws://{}:{}{}", host, port, path)
    
    async def _start_long_polling(self) -> None:
        """Start HTTP long polling for events (if supported by adapter)."""
        # Note: Most OneBot adapters use WebSocket reverse, not HTTP polling
        # This is a placeholder for adapters that support it
        logger.warning("HTTP long polling not fully implemented. Use WebSocket reverse instead.")
    
    async def _handle_onebot_event(self, event: dict[str, Any]) -> None:
        """Handle incoming OneBot event."""
        post_type = event.get("post_type")
        
        if post_type == "message":
            await self._handle_message_event(event)
        elif post_type == "notice":
            await self._handle_notice_event(event)
        elif post_type == "request":
            await self._handle_request_event(event)
        elif post_type == "meta_event":
            # Heartbeat, lifecycle events, etc. - ignore
            pass
    
    async def _handle_message_event(self, event: dict[str, Any]) -> None:
        """Handle OneBot message event."""
        message_type = event.get("message_type")
        user_id = event.get("user_id")
        message_id = event.get("message_id")
        raw_message = event.get("raw_message", "")
        
        # Skip self messages
        if self._bot_qq and user_id == self._bot_qq:
            return
        
        # Deduplicate
        if message_id and message_id in self._processed_ids:
            return
        if message_id:
            self._processed_ids.append(message_id)
        
        # Extract text content (simplified - OneBot messages can be complex)
        content = self._extract_text_from_message(event.get("message", []))
        if not content:
            return
        
        if message_type == "private":
            # Private message (C2C)
            await self._handle_message(
                sender_id=str(user_id),
                chat_id=str(user_id),
                content=content,
                metadata={
                    "message_id": message_id,
                    "message_type": "private",
                    "raw_message": raw_message,
                },
            )
        elif message_type == "group":
            # Group message
            group_id = event.get("group_id")
            # Check if bot is mentioned (OneBot format: @bot_name or CQ:at)
            is_at = self._is_bot_mentioned(event.get("message", []), self._bot_qq)
            
            if is_at or not self._bot_qq:  # If no bot_qq set, process all messages
                await self._handle_message(
                    sender_id=str(user_id),
                    chat_id=f"group_{group_id}",
                    content=content,
                    metadata={
                        "message_id": message_id,
                        "message_type": "group",
                        "group_id": group_id,
                        "raw_message": raw_message,
                        "is_at": is_at,
                    },
                )
    
    def _extract_text_from_message(self, message: list[dict[str, Any]] | str) -> str:
        """Extract text content from OneBot message array."""
        if isinstance(message, str):
            return message
        
        if not isinstance(message, list):
            return ""
        
        texts = []
        for segment in message:
            if isinstance(segment, dict):
                msg_type = segment.get("type")
                if msg_type == "text":
                    texts.append(segment.get("data", {}).get("text", ""))
                elif msg_type == "at":
                    # Convert @ mentions to readable format
                    qq = segment.get("data", {}).get("qq", "")
                    texts.append(f"@用户{qq} ")
            elif isinstance(segment, str):
                texts.append(segment)
        
        return "".join(texts).strip()
    
    def _is_bot_mentioned(self, message: list[dict[str, Any]], bot_qq: int | None) -> bool:
        """Check if bot is mentioned in message."""
        if not bot_qq:
            return False
        
        if not isinstance(message, list):
            return False
        
        for segment in message:
            if isinstance(segment, dict):
                msg_type = segment.get("type")
                if msg_type == "at":
                    qq = segment.get("data", {}).get("qq")
                    if str(qq) == str(bot_qq):
                        return True
        
        return False
    
    async def _handle_notice_event(self, event: dict[str, Any]) -> None:
        """Handle OneBot notice event (group member changes, etc.)."""
        notice_type = event.get("notice_type")
        logger.debug("OneBot notice event: {}", notice_type)
    
    async def _handle_request_event(self, event: dict[str, Any]) -> None:
        """Handle OneBot request event (friend requests, etc.)."""
        request_type = event.get("request_type")
        logger.debug("OneBot request event: {}", request_type)
    
    async def stop(self) -> None:
        """Stop the OneBot channel."""
        self._running = False
        
        # Close all WebSocket clients
        for ws in list(self._ws_clients):
            await ws.close()
        self._ws_clients.clear()
        
        # Stop WebSocket server
        if self._ws_site:
            await self._ws_site.stop()
            self._ws_site = None
        
        if self._ws_runner:
            await self._ws_runner.cleanup()
            self._ws_runner = None
        
        if self._session:
            await self._session.close()
            self._session = None
        
        logger.info("QQ OneBot channel stopped")
    
    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through OneBot API."""
        if not self._session:
            logger.warning("QQ OneBot session not initialized")
            return
        
        try:
            # Check if this is a group message
            if msg.chat_id.startswith("group_"):
                group_id = int(msg.chat_id.replace("group_", "", 1))
                await self._send_group_message(group_id, msg.content)
            else:
                # Private message
                user_id = int(msg.chat_id)
                await self._send_private_message(user_id, msg.content)
        except Exception as e:
            logger.error("Error sending QQ OneBot message: {}", e)
    
    async def _send_private_message(self, user_id: int, content: str) -> None:
        """Send private message via OneBot API."""
        api_endpoint = f"{self.config.api_url}/send_private_msg"
        
        payload = {
            "user_id": user_id,
            "message": content,
        }
        
        async with self._session.post(api_endpoint, json=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error("OneBot API error: {} - {}", resp.status, text)
                return
            
            result = await resp.json()
            if result.get("status") != "ok":
                logger.error("OneBot API returned error: {}", result.get("wording"))
    
    async def _send_group_message(self, group_id: int, content: str) -> None:
        """Send group message via OneBot API."""
        api_endpoint = f"{self.config.api_url}/send_group_msg"
        
        payload = {
            "group_id": group_id,
            "message": content,
        }
        
        async with self._session.post(api_endpoint, json=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error("OneBot API error: {} - {}", resp.status, text)
                return
            
            result = await resp.json()
            if result.get("status") != "ok":
                logger.error("OneBot API returned error: {}", result.get("wording"))
