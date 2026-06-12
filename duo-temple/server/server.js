import http from 'node:http';
import { WebSocketServer } from 'ws';
import crypto from 'node:crypto';

const PORT = process.env.PORT || 10000;
const rooms = new Map();

function uid(size = 6) {
  return crypto.randomBytes(size).toString('base64url').slice(0, size).toUpperCase();
}

function safeSend(ws, message) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(message));
}

function publicRoom(room) {
  return {
    id: room.id,
    players: Object.fromEntries(Object.entries(room.players).map(([role, ws]) => [role, Boolean(ws)])),
    createdAt: room.createdAt
  };
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }

  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Duo Temple WebSocket server is running. Use /health for status.');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.meta = { roomId: null, role: null, lastSeen: Date.now() };

  safeSend(ws, { type: 'hello', serverTime: Date.now() });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    ws.meta.lastSeen = Date.now();

    if (msg.type === 'create-room') {
      const roomId = uid(5);
      const room = { id: roomId, players: { fire: null, water: null }, createdAt: Date.now() };
      rooms.set(roomId, room);
      joinRoom(ws, roomId, msg.role === 'water' ? 'water' : 'fire');
      return;
    }

    if (msg.type === 'join-room') {
      const role = msg.role === 'fire' || msg.role === 'water' ? msg.role : null;
      joinRoom(ws, String(msg.roomId || '').toUpperCase().trim(), role);
      return;
    }

    const room = rooms.get(ws.meta.roomId);
    if (!room || !ws.meta.role) return;

    if (msg.type === 'input') {
      relay(room, ws, { type: 'peer-input', role: ws.meta.role, input: msg.input, seq: msg.seq, t: Date.now() });
    }

    if (msg.type === 'state') {
      relay(room, ws, { type: 'peer-state', role: ws.meta.role, state: msg.state, t: Date.now() });
    }

    if (msg.type === 'restart') {
      broadcast(room, { type: 'restart', by: ws.meta.role, t: Date.now() });
    }

    if (msg.type === 'level') {
      relay(room, ws, { type: 'peer-level', index: Number(msg.index) || 0, by: ws.meta.role, t: Date.now() });
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

function joinRoom(ws, roomId, requestedRole) {
  let room = rooms.get(roomId);
  if (!room) {
    safeSend(ws, { type: 'error', message: 'Комната не найдена' });
    return;
  }

  leave(ws);

  let role = requestedRole;
  if (!role || room.players[role]) role = !room.players.fire ? 'fire' : (!room.players.water ? 'water' : null);
  if (!role) {
    safeSend(ws, { type: 'error', message: 'Комната уже заполнена' });
    return;
  }

  room.players[role] = ws;
  ws.meta.roomId = room.id;
  ws.meta.role = role;

  safeSend(ws, { type: 'joined', room: publicRoom(room), role });
  broadcast(room, { type: 'room-update', room: publicRoom(room) });
}

function leave(ws) {
  const { roomId, role } = ws.meta || {};
  if (!roomId || !role) return;
  const room = rooms.get(roomId);
  if (room && room.players[role] === ws) {
    room.players[role] = null;
    broadcast(room, { type: 'room-update', room: publicRoom(room), left: role });
    if (!room.players.fire && !room.players.water) rooms.delete(roomId);
  }
  ws.meta.roomId = null;
  ws.meta.role = null;
}

function relay(room, sender, message) {
  for (const ws of Object.values(room.players)) {
    if (ws && ws !== sender) safeSend(ws, message);
  }
}

function broadcast(room, message) {
  for (const ws of Object.values(room.players)) if (ws) safeSend(ws, message);
}

setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of rooms) {
    for (const [role, ws] of Object.entries(room.players)) {
      if (ws && now - ws.meta.lastSeen > 120_000) {
        try { ws.close(); } catch {}
        room.players[role] = null;
      }
    }
    if (!room.players.fire && !room.players.water && now - room.createdAt > 300_000) rooms.delete(roomId);
  }
}, 30_000).unref();

server.listen(PORT, () => console.log(`Duo Temple server listening on ${PORT}`));
