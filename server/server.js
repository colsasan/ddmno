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
  },
  // 一時的な通信の乱れで即切断扱いにならないよう、ハートビートに余裕を持たせる
  pingInterval: 10000,
  pingTimeout: 30000
});

const queue = [];            // { id, name, party, key, joinedAt, lastPing, pingCapable }
const players = new Map();   // socketId -> { name, party, key, idleStrikes, idleKicked }
const matches = new Map();

// ===== マッチング・同期まわりの設定 =====
const PROPOSAL_TIMEOUT = 25 * 1000;     // マッチ成立後、双方が「対戦する」を選ぶまでの上限(超えたらサーバー側で破棄)
const QUEUE_STALE_MS = 20 * 1000;       // 検索中の生存確認(queue_ping)が途絶えたら待機列から外す
const TURN_TIMEOUT = 120 * 1000;        // 対戦中、片方が行動を送らないまま経過したら放置扱い
const RECONNECT_GRACE = 25 * 1000;      // 対戦中に切断されても、この時間内なら再接続して続行できる
const PAIR_COOLDOWN = 90 * 1000;        // 辞退・無応答だった組み合わせを再度マッチさせない期間
const MATCH_MAX_LIFETIME = 3 * 60 * 60 * 1000;
const IDLE_STRIKE_LIMIT = 2;            // 無応答がこの回数続いたら自動マッチングを止める(操作が戻るまで)
const recentPairs = new Map();          // "keyA|keyB" -> 期限

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
  try {
    const tmp = WEEKLY_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(weekly));
    fs.renameSync(tmp, WEEKLY_FILE); // 書き込み途中で落ちてもファイルが壊れないようにする
  } catch (e) { console.warn("[週間ランキング] 保存失敗:", e.message); }
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

function pairKey(a, b) {
  return [String(a), String(b)].sort().join("|");
}

function isPairOnCooldown(a, b) {
  const k = pairKey(a.key || a.id, b.key || b.id);
  const until = recentPairs.get(k);
  if (!until) return false;
  if (until <= Date.now()) { recentPairs.delete(k); return false; }
  return true;
}

function setPairCooldown(match) {
  if (!match || !match.keys) return;
  const ids = match.players;
  if (ids.length === 2) recentPairs.set(pairKey(match.keys[ids[0]] || ids[0], match.keys[ids[1]] || ids[1]), Date.now() + PAIR_COOLDOWN);
}

function removeFromQueue(socketId) {
  const index = queue.findIndex(p => p.id === socketId);
  if (index !== -1) queue.splice(index, 1);
}

// 切断済み・生存確認が途絶えた(放置された)待機者を待機列から外す
function pruneQueue() {
  const now = Date.now();
  for (let i = queue.length - 1; i >= 0; i--) {
    const q = queue[i];
    const sock = io.sockets.sockets.get(q.id);
    if (!sock || !players.has(q.id)) { queue.splice(i, 1); continue; }
    if (q.pingCapable && now - q.lastPing > QUEUE_STALE_MS) {
      queue.splice(i, 1);
      sock.emit("queue_expired");
      console.log(`[待機列から除外] 生存確認なし: ${q.name} (${q.id})`);
    }
  }
}

function leaveRoom(socketId, roomId) {
  const s = io.sockets.sockets.get(socketId);
  if (s) s.leave(roomId);
}

// 対戦(または承諾待ち)の破棄。claimRooms(週間勝利の報告受付)は残す
function endMatch(roomId) {
  const match = matches.get(roomId);
  if (!match) return null;
  clearTimeout(match.proposalTimer);
  matches.delete(roomId);
  for (const pId of match.players) leaveRoom(pId, roomId);
  return match;
}

function emitTo(socketId, event, payload) {
  const s = io.sockets.sockets.get(socketId);
  if (s) s.emit(event, payload);
}

function otherPlayer(match, socketId) {
  return match.players.find(p => p !== socketId);
}

function createMatch(p1, p2) {
  const s1 = io.sockets.sockets.get(p1.id);
  const s2 = io.sockets.sockets.get(p2.id);
  if (!s1 || !s2 || !players.has(p1.id) || !players.has(p2.id)) {
    // 片方が既にいない場合、残った側は待機列へ戻す
    if (s1 && players.has(p1.id)) queue.unshift(p1);
    if (s2 && players.has(p2.id)) queue.unshift(p2);
    return false;
  }

  const roomId = `match_${p1.id}_${p2.id}`;
  const match = {
    roomId,
    players: [p1.id, p2.id],
    keys: { [p1.id]: p1.key, [p2.id]: p2.key },
    playerData: {
      [p1.id]: { socketId: p1.id, name: p1.name, team: p1.party },
      [p2.id]: { socketId: p2.id, name: p2.name, team: p2.party }
    },
    turn: 1,
    actions: {},
    isResolving: false,
    started: false,
    decisions: {},
    lastResolution: {},
    disconnected: {},
    createdAt: Date.now(),
    lastProgress: Date.now(),
    proposalTimer: null
  };
  matches.set(roomId, match);
  claimRooms.set(roomId, { players: [p1.id, p2.id], names: { [p1.id]: p1.name, [p2.id]: p2.name }, started: false, claimed: new Set(), createdAt: Date.now() });

  s1.join(roomId);
  s2.join(roomId);

  // 双方が時間内に「対戦する」を選ばなければサーバー側で破棄する(放置対策)
  match.proposalTimer = setTimeout(() => expireProposal(roomId), PROPOSAL_TIMEOUT);

  s1.emit("matchFound", { roomId, playerId: p1.id, opponentName: p2.name, opponentTeam: p2.party });
  s2.emit("matchFound", { roomId, playerId: p2.id, opponentName: p1.name, opponentTeam: p1.party });

  console.log(`[マッチング成立] Room: ${roomId} (${p1.name} vs ${p2.name})`);
  return true;
}

function findMatch() {
  let again = true;
  while (again) {
    again = false;
    pruneQueue();
    outer:
    for (let i = 0; i < queue.length; i++) {
      for (let j = i + 1; j < queue.length; j++) {
        const a = queue[i], b = queue[j];
        if (a.id === b.id || (a.key && a.key === b.key)) continue; // 同一人物の多重接続同士は組ませない
        if (isPairOnCooldown(a, b)) continue;                       // 直前に辞退・無応答だった相手とは組ませない
        queue.splice(j, 1);
        queue.splice(i, 1);
        createMatch(a, b);
        again = true;
        break outer;
      }
    }
  }
}

function markIdle(socketId) {
  const pl = players.get(socketId);
  if (!pl) return;
  pl.idleStrikes = (pl.idleStrikes || 0) + 1;
  if (pl.idleStrikes >= IDLE_STRIKE_LIMIT) {
    pl.idleKicked = true;
    removeFromQueue(socketId);
    emitTo(socketId, "idle_kicked");
    console.log(`[放置判定] ${pl.name} (${socketId}) を自動マッチングから除外`);
  }
}

function expireProposal(roomId) {
  const match = matches.get(roomId);
  if (!match || match.started) return;
  console.log(`[承諾待ち時間切れ] Room=${roomId}`);
  for (const pId of match.players) {
    if (match.decisions[pId] === undefined) markIdle(pId); // 何も返さなかった側だけ放置扱い
  }
  setPairCooldown(match);
  for (const pId of match.players) {
    emitTo(pId, "match_expired", { roomId, reason: "timeout", noResponse: match.decisions[pId] === undefined });
  }
  endMatch(roomId);
}

// 対戦中の放置・切断の後始末(勝者側には opponent_disconnected を送る)
function finishAbandoned(match, loserIds, reason) {
  const roomId = match.roomId;
  endMatch(roomId);
  for (const pId of match.players) {
    if (loserIds.includes(pId)) {
      emitTo(pId, "match_timeout", { roomId, reason });
    } else {
      emitTo(pId, "opponent_disconnected", { roomId, reason });
    }
  }
  console.log(`[対戦終了(放置/切断)] Room=${roomId}, reason=${reason}`);
}

function sendResolution(match, pId, resolution) {
  emitTo(pId, "turn_resolution", resolution);
}

io.on("connection", socket => {
  console.log("Connected:", socket.id);

  socket.on("registerPlayer", data => {
    const name = cleanName(data?.name || data?.playerName);
    if (!name) {
      socket.emit("serverError", "プレイヤー名を入力してください");
      return;
    }
    const key = String(data?.playerId || data?.id || socket.id).slice(0, 64);
    const prev = players.get(socket.id);
    players.set(socket.id, { name, party: null, key, idleStrikes: prev?.idleStrikes || 0, idleKicked: !!prev?.idleKicked });
    socket.emit("registered", { playerId: socket.id, name });
    console.log(`[プレイヤー登録] ID: ${socket.id}, Name: ${name}`);
  });

  socket.on("joinQueue", data => {
    const player = players.get(socket.id);
    if (!player) {
      socket.emit("serverError", "先にプレイヤー登録をしてください");
      return;
    }
    if (player.idleKicked) {
      socket.emit("queue_blocked", { reason: "idle" });
      return;
    }
    const party = data?.party;
    if (!Array.isArray(party) || party.length !== 3) {
      socket.emit("serverError", "3匹の編成が必要です");
      return;
    }
    // 検索を始めたということは、前の対戦(承諾待ち含む)から離れたということ
    for (const [roomId, m] of matches) {
      if (!m.players.includes(socket.id)) continue;
      const other = otherPlayer(m, socket.id);
      endMatch(roomId);
      if (other) emitTo(other, "opponent_disconnected", { roomId, reason: "left" });
    }
    removeFromQueue(socket.id);
    player.party = party;
    const now = Date.now();
    queue.push({ id: socket.id, name: player.name, party, key: player.key, joinedAt: now, lastPing: now, pingCapable: data?.pingCapable === true });
    socket.emit("queueJoined");
    console.log(`[対戦待ちキュー追加] ID: ${socket.id}, Name: ${player.name}`);
    findMatch();
  });

  // 検索中の生存確認(クライアントが定期的に送る。途絶えたら待機列から外す)
  socket.on("queue_ping", () => {
    const q = queue.find(p => p.id === socket.id);
    if (q) { q.lastPing = Date.now(); q.pingCapable = true; }
  });

  // 画面操作が戻った合図 → 放置判定を解除
  socket.on("user_active", () => {
    const pl = players.get(socket.id);
    if (!pl) return;
    pl.idleStrikes = 0;
    pl.idleKicked = false;
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
    match.decisions[socket.id] = accept;

    const pl = players.get(socket.id);
    if (pl) pl.idleStrikes = 0; // 返答した = 放置ではない

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
      // 誰かが断ったらこのマッチを破棄(再検索は各クライアントが行う)。同じ相手とすぐ再マッチしないようにする
      setPairCooldown(match);
      endMatch(roomId);
    } else if (match.players.every(pId => match.decisions[pId] === true)) {
      match.started = true;
      match.lastProgress = Date.now();
      clearTimeout(match.proposalTimer);
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

  // --- 週間勝利数の復元: サーバーが再起動して記録が消えても、各プレイヤーの端末に残る今週の勝利数から復元する ---
  socket.on("sync_weekly_stats", data => {
    try {
      rollWeekly();
      if (!data || data.week !== weekly.week) return; // 今週の記録のみ受け付ける
      const wins = Math.floor(Number(data.wins));
      if (!Number.isFinite(wins) || wins <= 0 || wins > 5000) return;
      const pid = String(data.playerId || "").slice(0, 64);
      if (!pid) return;
      const registered = players.get(socket.id);
      const name = cleanName(registered?.name || data.playerName || data.name) || "名無し";
      const entry = weekly.players[pid];
      if (!entry) {
        weekly.players[pid] = { name, wins };
      } else if (entry.wins < wins) {
        entry.wins = wins;
        entry.name = name;
      } else {
        return; // サーバー側の方が多い(または同じ)なら何もしない
      }
      saveWeekly();
      console.log(`[週間勝利 復元] ${name} ${weekly.players[pid].wins}勝 (${weekly.week})`);
      io.emit("weekly_top", weeklyTopPayload());
    } catch (e) { console.warn("[sync_weekly_stats]", e.message); }
  });

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
    if (!match.started) {
      console.warn(`[submit_action] 対戦開始前の行動を無視: roomId=${roomId}`);
      return;
    }

    const action = data.action || { type: data.type, index: data.index };
    if (!action || (action.type !== "skill" && action.type !== "switch")) {
      console.warn(`[submit_action] 不正な行動を無視: ${JSON.stringify(action)}`);
      return;
    }

    // ターン番号がずれている行動(古い再送など)は採用しない。遅れているクライアントには前回の結果を再送する
    const sentTurn = Number(data.turn ?? data.turnNumber);
    if (Number.isFinite(sentTurn) && sentTurn !== match.turn) {
      console.warn(`[submit_action] ターン不一致を無視: received=${sentTurn}, server=${match.turn}`);
      if (sentTurn < match.turn && match.lastResolution[socket.id]?.turn === sentTurn) sendResolution(match, socket.id, match.lastResolution[socket.id]);
      return;
    }
    // 同一ターンの二重送信は最初の1回だけ採用(再送で行動が変わらないようにする)
    if (match.actions[socket.id]) {
      console.log(`[行動受信] 同ターンの再送を無視: Room=${roomId}, Player=${socket.id}`);
      return;
    }
    match.actions[socket.id] = { type: action.type, index: action.index };

    console.log(`[行動受信] Room=${roomId}, Player=${socket.id}, Action=${JSON.stringify(action)}, 揃った人数=${Object.keys(match.actions).length}/2`);

    // 2人分の行動が揃った場合、お互いの行動を交換してクライアントへ同期配信
    if (match.players.every(pId => match.actions[pId])) {
      if (match.isResolving) return;
      match.isResolving = true;

      const [id1, id2] = match.players;
      const act1 = match.actions[id1];
      const act2 = match.actions[id2];
      const resolvedTurn = match.turn;

      // アクションをリセットして次のターンへ備える
      match.actions = {};
      match.turn++;
      match.lastProgress = Date.now();
      match.isResolving = false;

      const res1 = { turn: resolvedTurn, myAction: act1, opponentAction: act2 };
      const res2 = { turn: resolvedTurn, myAction: act2, opponentAction: act1 };
      match.lastResolution[id1] = res1; // 取りこぼし時の再送用
      match.lastResolution[id2] = res2;

      sendResolution(match, id1, res1);
      sendResolution(match, id2, res2);

      console.log(`[ターン同期配信完了] Room=${roomId}, Turn=${resolvedTurn}`);
    }
  });

  // --- 同期の取りこぼし救済: 結果が届かないクライアントへ前回のターン結果を再送する ---
  socket.on("request_resync", data => {
    const match = matches.get(data?.roomId);
    if (!match || !match.started || !match.players.includes(socket.id)) return;
    const t = Number(data.turn);
    const last = match.lastResolution[socket.id];
    if (Number.isFinite(t) && t < match.turn && last && last.turn === t) {
      console.log(`[再同期] Room=${match.roomId}, Player=${socket.id}, Turn=${t} を再送`);
      sendResolution(match, socket.id, last);
    }
  });

  // --- 降参: 相手に通知して対戦を終了する(勝敗の報酬処理は各クライアントが行う) ---
  socket.on("surrender", data => {
    const roomId = data?.roomId;
    const match = matches.get(roomId);
    if (!match || !match.started || !match.players.includes(socket.id)) return;
    const other = otherPlayer(match, socket.id);
    endMatch(roomId);
    if (other) emitTo(other, "opponent_surrendered", { roomId });
    console.log(`[降参] Room=${roomId}, Player=${socket.id}`);
  });

  // --- 対戦が決着したクライアントからの終了報告(古い部屋が残って別の対戦に影響しないようにする) ---
  socket.on("match_finished", data => {
    const roomId = data?.roomId;
    const match = matches.get(roomId);
    if (!match || !match.players.includes(socket.id)) return;
    endMatch(roomId);
    console.log(`[対戦終了] Room=${roomId}`);
  });

  // --- 対戦中に回線が切れたクライアントの復帰 ---
  socket.on("rejoin_match", data => {
    const roomId = data?.roomId;
    const match = matches.get(roomId);
    const player = players.get(socket.id);
    if (!match || !match.started || !player) { socket.emit("rejoin_failed", { roomId }); return; }
    const oldId = match.players.find(pId => match.disconnected[pId] && match.keys[pId] === player.key);
    if (!oldId) { socket.emit("rejoin_failed", { roomId }); return; }

    const idx = match.players.indexOf(oldId);
    match.players[idx] = socket.id;
    match.keys[socket.id] = match.keys[oldId]; delete match.keys[oldId];
    match.playerData[socket.id] = { ...match.playerData[oldId], socketId: socket.id }; delete match.playerData[oldId];
    if (match.actions[oldId]) { match.actions[socket.id] = match.actions[oldId]; delete match.actions[oldId]; }
    if (match.decisions[oldId] !== undefined) { match.decisions[socket.id] = match.decisions[oldId]; delete match.decisions[oldId]; }
    if (match.lastResolution[oldId]) { match.lastResolution[socket.id] = match.lastResolution[oldId]; delete match.lastResolution[oldId]; }
    delete match.disconnected[oldId];

    const rec = claimRooms.get(roomId);
    if (rec) {
      rec.players = rec.players.map(p => (p === oldId ? socket.id : p));
      if (rec.names && rec.names[oldId] !== undefined) { rec.names[socket.id] = rec.names[oldId]; delete rec.names[oldId]; }
      if (rec.claimed.has(oldId)) { rec.claimed.delete(oldId); rec.claimed.add(socket.id); }
    }

    socket.join(roomId);
    socket.emit("rejoined", { roomId });
    const other = otherPlayer(match, socket.id);
    if (other) emitTo(other, "opponent_reconnected", { roomId });
    console.log(`[対戦復帰] Room=${roomId}, ${oldId} -> ${socket.id}`);

    // 切断中に進んだターンがあれば結果を再送(クライアント側でターン番号を見て無視/適用する)
    const last = match.lastResolution[socket.id];
    if (last) sendResolution(match, socket.id, last);
  });

  socket.on("disconnect", () => {
    removeFromQueue(socket.id);

    for (const [roomId, match] of matches) {
      if (!match.players.includes(socket.id)) continue;
      if (match.started) {
        // 対戦中: すぐには負けにせず、猶予時間内の再接続を待つ
        match.disconnected[socket.id] = Date.now();
        const other = otherPlayer(match, socket.id);
        if (other && !match.disconnected[other]) emitTo(other, "opponent_reconnecting", { roomId, graceMs: RECONNECT_GRACE });
        console.log(`[対戦中の切断] Room=${roomId} 再接続を最大${RECONNECT_GRACE / 1000}秒待機`);
      } else {
        // 承諾待ちの段階なら即破棄
        socket.to(roomId).emit("opponent_disconnected", { roomId, reason: "disconnect" });
        endMatch(roomId);
        console.log(`[承諾待ち中の切断] Room=${roomId} 破棄`);
      }
    }

    players.delete(socket.id);
    console.log("Disconnected:", socket.id);
  });
});

// 定期チェック: 検索中の放置・対戦中の切断/放置を掃除する
setInterval(() => {
  const now = Date.now();
  pruneQueue();

  for (const [roomId, match] of Array.from(matches)) {
    if (!matches.has(roomId)) continue;

    // 長時間残っている部屋は破棄(取りこぼし防止)
    if (now - match.createdAt > MATCH_MAX_LIFETIME) { endMatch(roomId); continue; }
    if (!match.started) continue;

    // 切断から猶予時間を過ぎた側は不戦敗
    const gone = match.players.filter(pId => match.disconnected[pId] && now - match.disconnected[pId] > RECONNECT_GRACE);
    if (gone.length) { finishAbandoned(match, gone, "disconnect"); continue; }

    // 行動を送らないまま長時間経過した側は放置扱い(両方放置なら引き分け扱いで終了)
    if (now - match.lastProgress > TURN_TIMEOUT) {
      const idle = match.players.filter(pId => !match.actions[pId] && !match.disconnected[pId]);
      if (idle.length) { finishAbandoned(match, idle, "timeout"); continue; }
    }
  }

  // 古い辞退記録を掃除
  for (const [k, until] of recentPairs) if (until <= now) recentPairs.delete(k);
}, 5000);

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
