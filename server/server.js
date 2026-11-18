const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const fs = require("fs");
const path = require("path");

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

// ===== 週間オンライン勝利数ランキング(サーバー集計・ファイル保存) =====
const WEEKLY_FILE = process.env.WEEKLY_FILE || path.join(__dirname, "weekly_wins.json");
const WEEKLY_TOP_N = 10;
const ROOM_CLAIM_TTL = 6 * 60 * 60 * 1000; // 勝利報告を受け付ける期間(対戦成立から6時間)
const claimRooms = new Map(); // roomId -> { players:[socketId,socketId], started, claimed:Set, createdAt }
let weekly = { week: "", players: {} }; // players: { [playerId]: { name, wins } }

// 週の判定は日本時間(月曜始まりのISO週)で行う
function currentWeekKey() {
  const d = new Date(Date.now() + 9 * 3600 * 1000);
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = t.getUTCDay() || 7;
  t.setUTCDate(t.getUTCDate() + 4 - day);
  const y = t.getUTCFullYear();
  const w = Math.ceil((((t - Date.UTC(y, 0, 1)) / 86400000) + 1) / 7);
  return y + "-W" + String(w).padStart(2, "0");
}

function loadWeekly() {
  try {
    const raw = JSON.parse(fs.readFileSync(WEEKLY_FILE, "utf8"));
    if (raw && typeof raw === "object" && raw.players && typeof raw.players === "object") weekly = raw;
  } catch (e) { /* ファイルが無い場合は空で開始 */ }
  rollWeekly();
}

function saveWeekly() {
  try { fs.writeFileSync(WEEKLY_FILE, JSON.stringify(weekly)); }
  catch (e) { console.warn("[週間ランキング] 保存失敗:", e.message); }
}

function rollWeekly() {
  const wk = currentWeekKey();
  if (weekly.week !== wk) {
    weekly = { week: wk, players: {} };
    saveWeekly();
  }
}

function weeklyTopPayload() {
  rollWeekly();
  const top = Object.values(weekly.players)
    .filter(e => e && e.wins > 0)
    .sort((a, b) => b.wins - a.wins)
    .slice(0, WEEKLY_TOP_N)
    .map(e => ({ name: e.name, wins: e.wins }));
  return { week: weekly.week, top };
}

loadWeekly();

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
    claimRooms.set(roomId, { players: [p1.id, p2.id], names: { [p1.id]: p1.name, [p2.id]: p2.name }, started: false, claimed: new Set(), createdAt: Date.now() });

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

  // --- マッチ成立後の「対戦する／しない」を相手に中継 ---
  socket.on("match_decision", data => {
    const roomId = data?.roomId;
    const match = matches.get(roomId);
    if (!match || !match.players.includes(socket.id)) return;
    if (match.started) return; // 双方承諾済み(対戦開始後)の遅延メッセージは無視

    const accept = data.accept === true;
    match.decisions = match.decisions || {};
    match.decisions[socket.id] = accept;

    // 送信者以外(相手)にだけ中継する
    socket.to(roomId).emit("match_decision", {
      roomId,
      from: socket.id,
      playerId: socket.id,
      accept,
      decision: accept ? "accept" : "decline"
    });
    console.log(`[対戦可否] Room=${roomId}, Player=${socket.id}, accept=${accept}`);

    if (!accept) {
      // 誰かが断ったらこのマッチを破棄(再検索は各クライアントが行う)
      matches.delete(roomId);
      for (const pId of match.players) {
        const ps = io.sockets.sockets.get(pId);
        if (ps) ps.leave(roomId);
      }
    } else if (match.players.every(pId => match.decisions[pId] === true)) {
      match.started = true;
      const rec = claimRooms.get(roomId);
      if (rec) rec.started = true;
    }
  });

  // --- 週間オンライン勝利数: 勝利報告(同じ部屋・同じプレイヤーの報告は1回だけ数える) ---
  const handleWeeklyWin = data => {
    const roomId = data?.roomId;
    const rec = claimRooms.get(roomId);
    if (!rec || !rec.started || !rec.players.includes(socket.id)) {
      socket.emit("weekly_ack", { roomId, accepted: false });
      return;
    }
    if (!rec.claimed.has(socket.id)) {
      rec.claimed.add(socket.id);
      rollWeekly();
      const registered = players.get(socket.id);
      const name = cleanName(registered?.name || rec.names?.[socket.id] || data?.playerName || data?.name) || "名無し";
      const pid = String(data?.playerId || socket.id).slice(0, 64);
      const entry = weekly.players[pid] || { name, wins: 0 };
      entry.name = name;
      entry.wins += 1;
      weekly.players[pid] = entry;
      saveWeekly();
      console.log(`[週間勝利] ${name} ${entry.wins}勝 (${weekly.week})`);
      io.emit("weekly_top", weeklyTopPayload());
    }
    socket.emit("weekly_ack", { roomId, accepted: true });
  };
  socket.on("report_weekly_win", handleWeeklyWin);
  socket.on("weekly_win", handleWeeklyWin);

  socket.on("get_weekly_top", () => {
    socket.emit("weekly_top", weeklyTopPayload());
  });
  socket.on("getWeeklyTop", () => {
    socket.emit("weekly_top", weeklyTopPayload());
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

// 古い勝利報告用レコードを定期的に掃除
setInterval(() => {
  const now = Date.now();
  for (const [roomId, rec] of claimRooms) {
    if (now - rec.createdAt > ROOM_CLAIM_TTL) claimRooms.delete(roomId);
  }
  rollWeekly();
}, 10 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`Online Relay Server running on port ${PORT}`);
});
