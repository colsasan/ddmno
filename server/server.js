
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: process.env.CLIENT_ORIGIN || "*",
    methods: ["GET", "POST"]
  }
});

const queue = [];
const players = new Map();
const matches = new Map();

app.get("/", (req, res) => {
  res.send("Pokemon Online Server is running!");
});

function cleanName(name) {
  if (typeof name !== "string") return "";
  return name.trim().slice(0, 12);
}

function removeFromQueue(socketId) {
  const index = queue.findIndex(p => p.id === socketId);
  if (index !== -1) queue.splice(index, 1);
}

function findMatch() {
  while (queue.length >= 2) {
    const p1 = queue.shift();
    const p2 = queue.shift();

    if (!players.has(p1.id) || !players.has(p2.id)) continue;
    if (p1.id === p2.id) continue;

    const roomId = `match_${p1.id}_${p2.id}`;

    const match = {
      roomId,
      players: [p1.id, p2.id],
      createdAt: Date.now()
    };

    matches.set(roomId, match);

    const s1 = io.sockets.sockets.get(p1.id);
    const s2 = io.sockets.sockets.get(p2.id);

    if (!s1 || !s2) {
      matches.delete(roomId);
      continue;
    }

    s1.join(roomId);
    s2.join(roomId);

    s1.emit("matchFound", {
      roomId,
      playerId: p1.id,
      opponent: {
        name: p2.name,
        party: p2.party
      }
    });

    s2.emit("matchFound", {
      roomId,
      playerId: p2.id,
      opponent: {
        name: p1.name,
        party: p1.party
      }
    });
  }
}

io.on("connection", socket => {
  console.log("Connected:", socket.id);

  socket.on("registerPlayer", data => {
    const name = cleanName(data?.name);

    if (!name) {
      socket.emit("serverError", "プレイヤー名を入力してください");
      return;
    }

    players.set(socket.id, {
      name,
      party: null
    });

    socket.emit("registered", {
      playerId: socket.id,
      name
    });
  });

  socket.on("joinQueue", data => {
    const player = players.get(socket.id);

    if (!player) {
      socket.emit("serverError", "先にプレイヤー登録をしてください");
      return;
    }

    const party = data?.party;

    if (!Array.isArray(party) || party.length !== 3) {
      socket.emit("serverError", "3匹の編成が必要です");
      return;
    }

    removeFromQueue(socket.id);

    player.party = party;

    queue.push({
      id: socket.id,
      name: player.name,
      party
    });

    socket.emit("queueJoined");
    findMatch();
  });

  socket.on("cancelQueue", () => {
    removeFromQueue(socket.id);
    socket.emit("queueCancelled");
  });

  socket.on("disconnect", () => {
    removeFromQueue(socket.id);

    for (const [roomId, match] of matches) {
      if (match.players.includes(socket.id)) {
        socket.to(roomId).emit("opponentDisconnected");
        matches.delete(roomId);
      }
    }

    players.delete(socket.id);
    console.log("Disconnected:", socket.id);
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});

