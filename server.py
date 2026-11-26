#!/usr/bin/env python3
"""SkyAtlas Mehrspieler-Server (nur Python-Standardbibliothek, Python 3.8+).

Leitet Positionen zwischen den Piloten eines Raums weiter und zählt Treffer/Abschüsse.
Räume: max. 8 Piloten. "Schnelles Spiel" landet in einem offenen Raum, "Raum erstellen" erzeugt
einen privaten Raum mit 4-stelligem Code.

Start:  python server.py            (Port 8765, oder Umgebungsvariable PORT)
Ein normaler HTTP-Aufruf (z. B. Health-Check des Hosters) bekommt "SkyAtlas server OK".
"""
import asyncio
import base64
import hashlib
import json
import os
import random
import struct
import time

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_PLAYERS = 8
CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
MAX_FRAME = 64 * 1024

rooms = {}
next_id = 1


class Client:
    def __init__(self, reader, writer):
        global next_id
        self.reader, self.writer = reader, writer
        self.id = next_id
        next_id += 1
        self.name = "Pilot"
        self.room = None
        self.hp = 100
        self.score = 0
        self.alive = True
        self.hit_times = []
        self.closed = False

    def info(self):
        return {"id": self.id, "name": self.name, "score": self.score}

    def send(self, obj):
        if self.closed:
            return
        try:
            self.writer.write(encode_frame(json.dumps(obj, separators=(",", ":")).encode()))
        except Exception:
            self.closed = True


class Room:
    def __init__(self, code, public):
        self.code, self.public = code, public
        self.clients = {}
        self.seed = random.randint(1, 10**9)

    def players(self):
        return [c.info() for c in self.clients.values()]

    def broadcast(self, obj, skip=None):
        data = encode_frame(json.dumps(obj, separators=(",", ":")).encode())
        for c in list(self.clients.values()):
            if c is skip or c.closed:
                continue
            try:
                c.writer.write(data)
            except Exception:
                c.closed = True


def new_code():
    while True:
        code = "".join(random.choice(CODE_CHARS) for _ in range(4))
        if code not in rooms:
            return code


# ---------------------------------------------------------------- WebSocket-Rahmen
def encode_frame(payload, opcode=1):
    head = bytes([0x80 | opcode])
    n = len(payload)
    if n < 126:
        head += bytes([n])
    elif n < 65536:
        head += bytes([126]) + struct.pack("!H", n)
    else:
        head += bytes([127]) + struct.pack("!Q", n)
    return head + payload


async def read_frame(reader):
    """Gibt (opcode, payload) einer vollständigen Nachricht zurück."""
    message, msg_op = b"", None
    while True:
        b1, b2 = await reader.readexactly(2)
        fin, opcode = b1 & 0x80, b1 & 0x0F
        masked, n = b2 & 0x80, b2 & 0x7F
        if n == 126:
            n = struct.unpack("!H", await reader.readexactly(2))[0]
        elif n == 127:
            n = struct.unpack("!Q", await reader.readexactly(8))[0]
        if n > MAX_FRAME:
            raise ConnectionError("frame too large")
        mask = await reader.readexactly(4) if masked else b"\0\0\0\0"
        data = bytearray(await reader.readexactly(n))
        for i in range(n):
            data[i] ^= mask[i & 3]
        if opcode >= 8:  # Steuerrahmen dürfen zwischen Fragmenten kommen
            return opcode, bytes(data)
        if opcode != 0:
            msg_op = opcode
        message += data
        if len(message) > MAX_FRAME:
            raise ConnectionError("message too large")
        if fin:
            return msg_op, message


async def handshake(reader, writer):
    raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
    if len(raw) > 8192:
        return False
    lines = raw.decode("latin-1").split("\r\n")
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    if headers.get("upgrade", "").lower() != "websocket" or "sec-websocket-key" not in headers:
        body = b"SkyAtlas server OK\n"
        # Access-Control-Allow-Origin: das Spiel (auf CrazyGames) darf prüfen, ob der Server wach ist
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-store\r\n"
                     b"Content-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)
        await writer.drain()
        return False
    accept = base64.b64encode(hashlib.sha1((headers["sec-websocket-key"] + GUID).encode()).digest()).decode()
    writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                  "Sec-WebSocket-Accept: " + accept + "\r\n\r\n").encode())
    await writer.drain()
    return True


# ---------------------------------------------------------------- Spiellogik
def join_room(c, mode, code):
    if mode == "join":
        room = rooms.get((code or "").upper())
        if not room or len(room.clients) >= MAX_PLAYERS:
            return None
    elif mode == "create":
        room = Room(new_code(), public=False)
        rooms[room.code] = room
    else:  # quick
        room = next((r for r in rooms.values() if r.public and len(r.clients) < MAX_PLAYERS), None)
        if not room:
            room = Room(new_code(), public=True)
            rooms[room.code] = room
    room.clients[c.id] = c
    c.room = room
    return room


def leave(c):
    room = c.room
    if not room:
        return
    room.clients.pop(c.id, None)
    c.room = None
    if room.clients:
        room.broadcast({"t": "leave", "id": c.id, "players": room.players()})
    else:
        rooms.pop(room.code, None)


def handle(c, msg):
    t = msg.get("t")
    room = c.room
    if t == "hello" and room is None:
        c.name = str(msg.get("name") or "Pilot")[:14] or "Pilot"
        room = join_room(c, msg.get("mode"), msg.get("code"))
        if room is None:
            c.send({"t": "error", "msg": "join_failed"})
            return
        c.send({"t": "welcome", "id": c.id, "code": room.code, "seed": room.seed, "players": room.players()})
        room.broadcast({"t": "join", "player": c.info(), "players": room.players()}, skip=c)
        return
    if room is None:
        return
    if t == "s":
        out = {"t": "s", "id": c.id, "p": msg.get("p"), "q": msg.get("q"), "v": msg.get("v"), "b": msg.get("b", 0), "a": msg.get("a", 1)}
        room.broadcast(out, skip=c)
    elif t == "hit":
        now = time.monotonic()
        c.hit_times = [x for x in c.hit_times if now - x < 1.0]
        if len(c.hit_times) >= 25 or not c.alive:
            return
        c.hit_times.append(now)
        target = room.clients.get(msg.get("target"))
        if not target or target is c or not target.alive:
            return
        dmg = max(0, min(100, int(msg.get("dmg", 0))))
        target.hp -= dmg
        target.send({"t": "hp", "hp": max(target.hp, 0)})
        if target.hp <= 0:
            target.alive = False
            c.score += 1
            room.broadcast({"t": "killed", "id": target.id, "by": c.id, "players": room.players()})
    elif t == "crash":
        if c.alive:
            c.alive = False
            room.broadcast({"t": "killed", "id": c.id, "by": None, "players": room.players()})
    elif t == "spawn":
        c.alive, c.hp = True, 100
        c.send({"t": "hp", "hp": 100})


async def client_task(reader, writer):
    c = Client(reader, writer)
    try:
        if not await handshake(reader, writer):
            return
        last_ping = time.monotonic()
        while True:
            try:
                op, data = await asyncio.wait_for(read_frame(reader), timeout=25)
            except asyncio.TimeoutError:
                writer.write(encode_frame(b"", 9))  # Ping
                last_ping = time.monotonic()
                continue
            if op == 8:
                break
            if op == 9:
                writer.write(encode_frame(data, 10))
                continue
            if op != 1:
                continue
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            if isinstance(msg, dict):
                handle(c, msg)
            await writer.drain()
            if time.monotonic() - last_ping > 25:
                writer.write(encode_frame(b"", 9))
                last_ping = time.monotonic()
    except (asyncio.IncompleteReadError, ConnectionError, asyncio.TimeoutError, asyncio.LimitOverrunError, OSError):
        pass
    finally:
        c.closed = True
        leave(c)
        try:
            writer.close()
        except Exception:
            pass


async def main():
    port = int(os.environ.get("PORT", "8765"))
    server = await asyncio.start_server(client_task, "0.0.0.0", port)
    print(f"SkyAtlas-Server läuft auf Port {port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
