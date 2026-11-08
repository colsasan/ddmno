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

// ==========================================
// --- 既存バトルロジックのサーバー移植 ---
// ==========================================
const TYPE_CHART = {
  kusa: { mizu: 2, honoo: 0.5, kusa: 0.5, jimen: 2, hikou: 0.5, doku: 0.5 },
  honoo: { kusa: 2, honoo: 0.5, mizu: 0.5 },
  mizu: { honoo: 2, kusa: 0.5, mizu: 0.5, jimen: 2 },
  normal: { ghost: 0 },
  kakuto: { normal: 2, aku: 2, doku: 0.5, hikou: 0.5, ghost: 0 },
  aku: { ghost: 2, aku: 0.5, kakuto: 0.5 },
  doku: { kusa: 2, doku: 0.5, jimen: 0.5, ghost: 0.5 },
  denki: { mizu: 2, hikou: 2, denki: 0.5, kusa: 0.5, jimen: 0 },
  jimen: { honoo: 2, denki: 2, doku: 2, kusa: 0.5, hikou: 0 },
  hikou: { kusa: 2, kakuto: 2, denki: 0.5 },
  ghost: { ghost: 2, normal: 0, aku: 0.5 }
};

function getTypeMultiplier(attackType, defenderElements) {
  let multi = 1;
  const defs = Array.isArray(defenderElements) ? defenderElements : [defenderElements];
  defs.forEach(def => {
    if (TYPE_CHART[attackType] && TYPE_CHART[attackType][def] !== undefined) {
      multi *= TYPE_CHART[attackType][def];
    }
  });
  return multi;
}

function getStageMultiplier(stage) {
  if (stage === 1) return 1.5;
  if (stage === 2) return 2.0;
  if (stage >= 3) return 3.0;
  if (stage === -1) return 1 / 1.5;
  if (stage === -2) return 0.5;
  if (stage <= -3) return 1 / 3.0;
  return 1.0;
}

function getPriority(mon, s) {
  if (!s) return 0;
  if (mon.ability && mon.ability.name === '絶対命令' && mon.hp === mon.maxHp && s.type === 'ghost') return 1;
  if (s.name === 'ねこだまし' || s.name === 'はやてがえし') return 3;
  if (s.name === 'しんそく') return 2;
  if (s.name === 'アクアジェット' || s.name === 'しんくうは' || s.name === 'マッハパンチ') return 1;
  return 0;
}

function triggerOnEntryAbility(mon, logs) {
  if (!mon || mon.hp <= 0) return;
  if (mon.ability && mon.ability.name === '荒い鼻息') {
    mon.stages.atk = Math.min(3, (mon.stages.atk || 0) + 1);
    logs.push(`[特性発動: ${mon.name}の荒い鼻息] 攻撃が1段階上がった！`);
  }
}

function findMatch() {
  while (queue.length >= 2) {
    const p1 = queue.shift();
    const p2 = queue.shift();

    if (!players.has(p1.id) || !players.has(p2.id)) continue;
    if (p1.id === p2.id) continue;

    const roomId = `match_${p1.id}_${p2.id}`;

    // パーティのディープコピーと初期化
    const team1 = JSON.parse(JSON.stringify(p1.party));
    const team2 = JSON.parse(JSON.stringify(p2.party));

    [team1, team2].forEach(team => {
      team.forEach(m => {
        m.hp = m.maxHp;
        m.stages = { atk: 0, def: 0, spa: 0, spd: 0, speed: 0 };
        m.status = null;
        m.statusTurns = 0;
        m.turnsInBattle = 0;
        m.hasSturdied = false;
        m.isSkipTurn = false;
        m.isProtected = false;
        m.isFirstTurn = true;
      });
    });

    const match = {
      roomId,
      players: [p1.id, p2.id],
      playerData: {
        [p1.id]: { socketId: p1.id, name: p1.name, team, activeIndex: 0 },
        [p2.id]: { socketId: p2.id, name: p2.name, team: team2, activeIndex: 0 }
      },
      turn: 1,
      actions: {},
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
      opponentName: p2.name,
      opponentTeam: team2
    });

    s2.emit("matchFound", {
      roomId,
      playerId: p2.id,
      opponentName: p1.name,
      opponentTeam: team1
    });
  }
}

// サーバー側ターン解決エンジン
function resolveTurn(match) {
  const pIds = match.players;
  const id1 = pIds[0];
  const id2 = pIds[1];

  const pData1 = match.playerData[id1];
  const pData2 = match.playerData[id2];

  const act1 = match.actions[id1];
  const act2 = match.actions[id2];

  match.actions = {}; // アクションリセット

  const logs = [];
  let isGameOver = false;
  let winnerId = null;

  const mon1 = pData1.team[pData1.activeIndex];
  const mon2 = pData2.team[pData2.activeIndex];

  // 交代アクションの事前処理
  if (act1 && act1.type === 'switch') {
    pData1.activeIndex = act1.index;
    const newMon = pData1.team[act1.index];
    newMon.isFirstTurn = true;
    logs.push(`[交代] ${pData1.name} は ${newMon.name} に交代した！`);
    triggerOnEntryAbility(newMon, logs);
  }
  if (act2 && act2.type === 'switch') {
    pData2.activeIndex = act2.index;
    const newMon = pData2.team[act2.index];
    newMon.isFirstTurn = true;
    logs.push(`[交代] ${pData2.name} は ${newMon.name} に交代した！`);
    triggerOnEntryAbility(newMon, logs);
  }

  const currentMon1 = pData1.team[pData1.activeIndex];
  const currentMon2 = pData2.team[pData2.activeIndex];

  // 技選択時の行動順計算
  let s1 = currentMon1.speed * getStageMultiplier(currentMon1.stages.speed);
  let s2 = currentMon2.speed * getStageMultiplier(currentMon2.stages.speed);
  if (currentMon1.status === 'mahi') s1 *= 0.5;
  if (currentMon2.status === 'mahi') s2 *= 0.5;

  const skill1 = (act1 && act1.type === 'skill') ? currentMon1.skills[act1.index] : null;
  const skill2 = (act2 && act2.type === 'skill') ? currentMon2.skills[act2.index] : null;

  let p1Priority = getPriority(currentMon1, skill1);
  let p2Priority = getPriority(currentMon2, skill2);

  let firstIs1 = (p1Priority > p2Priority) || (p1Priority === p2Priority && s1 >= s2);

  const executeAction = (attacker, defender, act, isFirst) => {
    if (attacker.hp <= 0 || !act) return;
    if (act.type === 'switch') return; // 交代済み

    if (act.type === 'skill') {
      const skill = attacker.skills[act.index];
      if (!skill || skill.pp <= 0) {
        logs.push(`${attacker.name} は技をだせなかった！`);
        return;
      }
      skill.pp--;
      logs.push(`${attacker.name} の ${skill.name}！`);

      if (skill.category === 'status') {
        if (skill.name === 'つるぎのまい') {
          attacker.stages.atk = Math.min(3, attacker.stages.atk + 1);
          logs.push(`${attacker.name} の攻撃が1段階上がった！`);
        } else if (skill.name === 'まもる') {
          attacker.isProtected = true;
          logs.push(`${attacker.name} は身を守っている。`);
        } else {
          logs.push(`しかし うまく決まらなかった！`);
        }
      } else {
        const typeMulti = getTypeMultiplier(skill.type, defender.element);
        if (typeMulti === 0) {
          logs.push(`効果がないようだ…`);
        } else {
          let atkStat = skill.category === 'physical' ? attacker.atk * getStageMultiplier(attacker.stages.atk) : attacker.spa * getStageMultiplier(attacker.stages.spa);
          let defStat = skill.category === 'physical' ? defender.def * getStageMultiplier(defender.stages.def) : defender.spd * getStageMultiplier(defender.stages.spd);

          let raw = (((skill.power * atkStat / defStat) / 50) + 2) * (0.85 + Math.random() * 0.15) * typeMulti * 4;
          let dmg = Math.max(1, Math.floor(raw));

          if (defender.hp - dmg <= 0) {
            if (defender.equippedItemId === 'kiai_tasuki') {
              defender.equippedItemId = null;
              dmg = defender.hp - 1;
              logs.push(`[きあいのタスキ] ${defender.name} はHP1で耐えた！`);
            } else if (defender.ability && defender.ability.name === '怒涛の踏ん張り' && !defender.hasSturdied) {
              defender.hasSturdied = true;
              dmg = defender.hp - 1;
              logs.push(`[怒涛の踏ん張り] ${defender.name} はHP1で耐えた！`);
            }
          }

          defender.hp = Math.max(0, defender.hp - dmg);
          logs.push(`${defender.name} に ${dmg} のダメージ！`);
        }
      }
    }
  };

  if (firstIs1) {
    executeAction(currentMon1, currentMon2, act1, true);
    if (currentMon2.hp > 0) {
      executeAction(currentMon2, currentMon1, act2, false);
    }
  } else {
    executeAction(currentMon2, currentMon1, act2, true);
    if (currentMon1.hp > 0) {
      executeAction(currentMon1, currentMon2, act1, false);
    }
  }

  // ひん死および自動交代チェック
  if (currentMon2.hp <= 0) {
    logs.push(`相手の ${currentMon2.name} は倒れた！`);
    const nextIdx = pData2.team.findIndex(m => m.hp > 0);
    if (nextIdx !== -1) {
      pData2.activeIndex = nextIdx;
      pData2.team[nextIdx].isFirstTurn = true;
      logs.push(`相手は ${pData2.team[nextIdx].name} を繰り出した！`);
    } else {
      isGameOver = true;
      winnerId = id1;
    }
  }

  if (currentMon1.hp <= 0) {
    logs.push(`${currentMon1.name} は倒れてしまった！`);
    const nextIdx = pData1.team.findIndex(m => m.hp > 0);
    if (nextIdx !== -1) {
      pData1.activeIndex = nextIdx;
      pData1.team[nextIdx].isFirstTurn = true;
      logs.push(`いけっ！ ${pData1.team[nextIdx].name}！`);
    } else {
      isGameOver = true;
      winnerId = id2;
    }
  }

  match.turn++;

  // 両プレイヤーへ結果を返す
  const s1 = io.sockets.sockets.get(id1);
  const s2 = io.sockets.sockets.get(id2);

  const resData1 = {
    turn: match.turn,
    logs,
    playerHp: currentMon1.hp,
    opponentHp: currentMon2.hp,
    playerActiveIndex: pData1.activeIndex,
    opponentActiveIndex: pData2.activeIndex,
    isGameOver,
    winnerId: winnerId === id1 ? pData1.name : (winnerId === id2 ? pData2.name : null)
  };

  const resData2 = {
    turn: match.turn,
    logs,
    playerHp: currentMon2.hp,
    opponentHp: currentMon1.hp,
    playerActiveIndex: pData2.activeIndex,
    opponentActiveIndex: pData1.activeIndex,
    isGameOver,
    winnerId: winnerId === id2 ? pData2.name : (winnerId === id1 ? pData1.name : null)
  };

  if (s1) s1.emit("turn_resolution", resData1);
  if (s2) s2.emit("turn_resolution", resData2);

  if (isGameOver) {
    matches.delete(match.roomId);
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
    players.set(socket.id, { name, party: null });
    socket.emit("registered", { playerId: socket.id, name });
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
    findMatch();
  });

  socket.on("cancelQueue", () => {
    removeFromQueue(socket.id);
    socket.emit("queueCancelled");
  });

  // --- 技選択イベントの受信処理 ---
  socket.on("submit_action", data => {
    const roomId = data?.roomId;
    const match = matches.get(roomId);

    if (!match || !match.players.includes(socket.id)) {
      socket.emit("serverError", "有効な対戦ルームが見つかりません");
      return;
    }

    // プレイヤーの選択を保存
    match.actions[socket.id] = {
      type: data.action?.type || data.type, // 'skill' または 'switch'
      index: data.action?.index !== undefined ? data.action.index : data.index
    };

    // 2人分の選択が揃ったか判定
    if (match.players.every(pId => match.actions[pId])) {
      resolveTurn(match);
    }
  });

  socket.on("disconnect", () => {
    removeFromQueue(socket.id);

    for (const [roomId, match] of matches) {
      if (match.players.includes(socket.id)) {
        socket.to(roomId).emit("opponent_disconnected");
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
