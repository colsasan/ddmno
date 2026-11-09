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
  res.send("Pokemon Online Relay Server is running!");
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
      playerData: {
        [p1.id]: { socketId: p1.id, name: p1.name, team: p1.party },
        [p2.id]: { socketId: p2.id, name: p2.name, team: p2.party }
      },
      turn: 1,
      actions: {},
      isResolving: false,
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

    // 相手のチーム情報を添えてマッチ成立を通知
    s1.emit("matchFound", {
      roomId,
      playerId: p1.id,
      opponentName: p2.name,
      opponentTeam: p2.party
    });

    s2.emit("matchFound", {
      roomId,
      playerId: p2.id,
      opponentName: p1.name,
      opponentTeam: p1.party
    });

    console.log(`[マッチング成立] Room: ${roomId} (${p1.name} vs ${p2.name})`);
  }
}

io.on("connection", socket => {
  console.log("Connected:", socket.id);

  socket.on("registerPlayer", data => {
    const name = cleanName(data?.name || data?.playerName);
    if (!name) {
      socket.emit("serverError", "プレイヤー名を入力してください");
      return;
    }
    players.set(socket.id, { name, party: null });
    socket.emit("registered", { playerId: socket.id, name });
    console.log(`[プレイヤー登録] ID: ${socket.id}, Name: ${name}`);
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
    queue.push({ id: socket.id, name: player.name, party });
    socket.emit("queueJoined");
    console.log(`[対戦待ちキュー追加] ID: ${socket.id}, Name: ${player.name}`);
    findMatch();
  });

  socket.on("cancel_match", () => {
    removeFromQueue(socket.id);
    socket.emit("queueCancelled");
    console.log(`[キューキャンセル] ID: ${socket.id}`);
  });

  // --- クライアント主導型：行動の受信と同期中継 ---
  socket.on("submit_action", data => {
    const roomId = data?.roomId;
    const match = matches.get(roomId);

    if (!match || !match.players.includes(socket.id)) {
      console.warn(`[submit_action] 無効なルームまたはプレイヤー: roomId=${roomId}, socketId=${socket.id}`);
      socket.emit("serverError", "有効な対戦ルームが見つかりません");
      return;
    }

    const action = data.action || { type: data.type, index: data.index };
    match.actions[socket.id] = action;

    console.log(`[行動受信] Room=${roomId}, Player=${socket.id}, Action=${JSON.stringify(action)}, 揃った人数=${Object.keys(match.actions).length}/2`);

    // 2人分の行動が揃った場合、お互いの行動を交換してクライアントへ同期配信
    if (match.players.every(pId => match.actions[pId])) {
      if (match.isResolving) return;
      match.isResolving = true;

      const pIds = match.players;
      const id1 = pIds[0];
      const id2 = pIds[1];

      const act1 = match.actions[id1];
      const act2 = match.actions[id2];

      // アクションをリセットして次のターンへ備える
      match.actions = {};
      match.isResolving = false;
      match.turn++;

      const s1 = io.sockets.sockets.get(id1);
      const s2 = io.sockets.sockets.get(id2);

      const resolutionDataFor1 = {
        turn: match.turn - 1,
        myAction: act1,
        opponentAction: act2
      };

      const resolutionDataFor2 = {
        turn: match.turn - 1,
        myAction: act2,
        opponentAction: act1
      };

      if (s1) s1.emit("turn_resolution", resolutionDataFor1);
      if (s2) s2.emit("turn_resolution", resolutionDataFor2);

      console.log(`[ターン同期配信完了] Room=${roomId}, Turn=${match.turn - 1}`);
    }
  });

  socket.on("disconnect", () => {
    removeFromQueue(socket.id);

    for (const [roomId, match] of matches) {
      if (match.players.includes(socket.id)) {
        socket.to(roomId).emit("opponent_disconnected");
        matches.delete(roomId);
        console.log(`[対戦切断] Room=${roomId} 破棄`);
      }
    }

    players.delete(socket.id);
    console.log("Disconnected:", socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`Online Relay Server running on port ${PORT}`);
});
