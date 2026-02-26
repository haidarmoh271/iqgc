'use strict';

const express     = require('express');
const http        = require('http');
const { Server }  = require('socket.io');
const path        = require('path');
const QRCode      = require('qrcode');
const compression = require('compression');

// ══════════════════════════════════════════
//  CONFIG
// ══════════════════════════════════════════
const PORT      = process.env.PORT || 3000;
const MAX_ROOMS = parseInt(process.env.MAX_ROOMS || '50', 10);

// [01] HIGH: لا fallback لكلمة السر — لو غير مضبوط السيرفر يرفض الشغل
// السبب: '120' موجودة في git history ومعروفة — fallback = ثغرة مضمونة
// الحل: أضف ADMIN_PASS في Railway → Variables قبل الـ deploy
if (!process.env.ADMIN_PASS) {
  console.error('[FATAL] ADMIN_PASS environment variable is not set.');
  console.error('[FATAL] Set it in Railway → Variables → Add ADMIN_PASS');
  process.exit(1);
}
const ADMIN_PASS = process.env.ADMIN_PASS;
const GRACE_MS   = 20_000; // grace period reconnect
const AI_TIMEOUT = 30_000; // timeout لطلبات Anthropic

// ══════════════════════════════════════════
//  LOGGER — structured, timestamped
// ══════════════════════════════════════════
const log = {
  _fmt: (level, msg, meta) => {
    const ts  = new Date().toISOString();
    const str = meta ? ` ${JSON.stringify(meta)}` : '';
    return `[${ts}] [${level}] ${msg}${str}`;
  },
  info:  (msg, meta) => console.log(log._fmt('INFO ', msg, meta)),
  warn:  (msg, meta) => console.warn(log._fmt('WARN ', msg, meta)),
  error: (msg, meta) => console.error(log._fmt('ERROR', msg, meta)),
  event: (event, meta) => console.log(log._fmt('EVENT', event, meta)),
};

// ══════════════════════════════════════════
//  CRASH GUARDS — يجب أن يكونوا أول شيء
// ══════════════════════════════════════════
process.on('uncaughtException', (err) => {
  log.error('uncaughtException — سيتوقف السيرفر', { message: err.message, stack: err.stack });
  // أعطِ السيرفر فرصة لتسجيل الخطأ ثم أعد التشغيل (Railway سيعيد تشغيله)
  setTimeout(() => process.exit(1), 500);
});

process.on('unhandledRejection', (reason, promise) => {
  log.error('unhandledRejection — لم تُعالَج', {
    reason: reason instanceof Error ? reason.message : String(reason),
  });
  // لا نُسقط السيرفر هنا — نسجّل فقط
});

// ══════════════════════════════════════════
//  EXPRESS + SERVER
// ══════════════════════════════════════════
const app    = express();
const server = http.createServer(app);

// ── Socket.IO — مضبوط لـ 120+ لاعب ──
const io = new Server(server, {
  // حد أقصى لحجم رسالة واحدة = 64KB (افتراضي 1MB)
  // يمنع لاعباً يرسل payload ضخم يأكل RAM
  maxHttpBufferSize: 64 * 1024,

  // ضغط WebSocket — يقلل bandwidth بـ 60-70% على رسائل leaderboard
  // مهم لـ 120 لاعب يستقبلون نفس البيانات في آن واحد
  perMessageDeflate: {
    threshold: 512, // اضغط فقط لو الرسالة > 512 bytes
  },

  // ping كل 25 ثانية — يكتشف الاتصالات الميتة بسرعة
  // مهم لإطلاق socket.id القديمة من الذاكرة
  pingInterval: 25_000,
  pingTimeout:  20_000,

  // تحديد transports — منع polling الذي يُنشئ HTTP requests مستمرة
  // WebSocket فقط = أقل overhead على الـ CPU
  transports: ['websocket', 'polling'], // polling كـ fallback فقط

  // حد أقصى للاتصالات المتزامنة = MAX_ROOMS × 130 لاعب + مضيفين
  // يمنع استنزاف الـ file descriptors
  allowUpgrades: true,
  upgradeTimeout: 10_000,
});

// ── HTTP Compression — يضغط HTML/JSON قبل الإرسال ──
// static files كـ host.html (65KB) تنزل إلى ~15KB
// مهم جداً لـ 120 لاعب يفتحون play.html في نفس الوقت
app.use(compression({
  level: 6,          // توازن بين سرعة الضغط واستهلاك CPU
  threshold: 1024,   // اضغط فقط لو الـ response > 1KB
}));

app.use(express.json({ limit: '100kb' }));
app.use(express.static(path.join(__dirname, 'public'), {
  // cache static files في المتصفح — يقلل الطلبات المتكررة
  maxAge: '1h',
  etag: true,
}));

// ── HTTP Keep-Alive — يُبقي TCP connections مفتوحة ──
// بدونه: كل QR request = TCP handshake جديد (مهم لـ 120 لاعب يسحبون QR)
server.keepAliveTimeout = 65_000;  // أكبر من timeout Railway (60s)
server.headersTimeout   = 66_000;

// ── Health Check — Railway يستخدمه لمعرفة إذا السيرفر حي ──
app.get('/health', (req, res) => {
  const mem = process.memoryUsage();
  res.json({
    status:      'ok',
    uptime:      Math.floor(process.uptime()),
    rooms:       Object.keys(rooms).length,
    dotsRooms:   Object.keys(dotsRooms).length,
    players:     Object.values(rooms).reduce((n, r) => n + Object.keys(r.players).length, 0),
    memory: {
      heapUsed:  Math.round(mem.heapUsed  / 1024 / 1024) + 'MB',
      heapTotal: Math.round(mem.heapTotal / 1024 / 1024) + 'MB',
      rss:       Math.round(mem.rss       / 1024 / 1024) + 'MB',
      external:  Math.round(mem.external  / 1024 / 1024) + 'MB',
    },
    node:        process.version,
    ts:          new Date().toISOString(),
  });
});

// ── AI Proxy — Anthropic مع Web Search ──
app.post('/ai-recent', (req, res) => {
  const { topic: rawTopic, count, difficulty } = req.body || {};
  const apiKey = process.env.ANTHROPIC_API_KEY || '';

  if (!apiKey) return res.status(503).json({ error: 'ANTHROPIC_API_KEY غير مضبوط' });
  if (!rawTopic) return res.status(400).json({ error: 'الموضوع مطلوب' });

  // Sanitize — منع prompt injection
  const topic = String(rawTopic)
    .slice(0, 80)
    .replace(/[\"\\]/g, '')
    .replace(/(\bignore\b|\bforget\b|\bsystem\b)/gi, '');

  const safeCount = Math.min(Math.max(parseInt(count, 10) || 5, 1), 15);
  const safeDiff  = ['سهل','متوسط','صعب'].includes(difficulty) ? difficulty : 'متوسط';

  const bodyStr = JSON.stringify({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 4000,
    tools: [{ type: 'web_search_20250305', name: 'web_search' }],
    messages: [{ role: 'user', content:
      `ابحث عن أحدث الأخبار عن "${topic}" ثم أنشئ ${safeCount} سؤال اختيار من متعدد باللغة العربية. مستوى الصعوبة: ${safeDiff}. قواعد: 4 خيارات، خيار واحد صحيح. أجب بـ JSON فقط: [{"question":"...","answers":["...","...","...","..."],"correct":0}]`
    }]
  });

  const https = require('https');
  const options = {
    hostname: 'api.anthropic.com',
    path: '/v1/messages',
    method: 'POST',
    headers: {
      'Content-Type':    'application/json',
      'Content-Length':  Buffer.byteLength(bodyStr),
      'x-api-key':       apiKey,
      'anthropic-version': '2023-06-01',
      'anthropic-beta':  'web-search-2025-03-05',
    },
  };

  let responded = false;
  const safeRespond = (code, body) => {
    if (responded) return;
    responded = true;
    res.status(code).json(body);
  };

  // Timeout — منع التعليق إذا Anthropic ما رد
  const timer = setTimeout(() => {
    log.warn('/ai-recent timeout', { topic });
    apiReq.destroy();
    safeRespond(504, { error: 'انتهت مهلة الاتصال بـ AI — حاول مرة أخرى' });
  }, AI_TIMEOUT);

  const apiReq = https.request(options, (apiRes) => {
    let raw = '';
    apiRes.on('data', chunk => raw += chunk);
    apiRes.on('end', () => {
      clearTimeout(timer);
      try {
        const data = JSON.parse(raw);
        if (data.type === 'error') {
          log.warn('/ai-recent API error', { msg: data.error?.message });
          return safeRespond(500, { error: data.error?.message || 'Anthropic error' });
        }
        const texts = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('');
        const match = texts.match(/\[[\s\S]*?\]/);
        if (!match) {
          log.warn('/ai-recent no JSON in response', { preview: texts.slice(0, 100) });
          return safeRespond(500, { error: 'لم يُنتج AI أسئلة — حاول مرة أخرى' });
        }
        const questions = JSON.parse(match[0]);
        safeRespond(200, { questions });
      } catch (e) {
        log.error('/ai-recent parse error', { message: e.message });
        safeRespond(500, { error: 'خطأ في معالجة الرد' });
      }
    });
  });

  apiReq.on('error', (e) => {
    clearTimeout(timer);
    log.error('/ai-recent request error', { message: e.message });
    safeRespond(500, { error: 'خطأ في الاتصال بـ AI' });
  });

  apiReq.write(bodyStr);
  apiReq.end();
});

// ══════════════════════════════════════════
//  HOST AUTH — server-side token system
//  الكلمة لا تُقارَن في الكلاينت أبداً
// ══════════════════════════════════════════
const hostSessions = new Set(); // tokens صالحة في الذاكرة

// [03] MEDIUM: Rate limiting على host auth لمنع brute force
// بدونه: أي شخص يستطيع 1000 محاولة في الثانية لتخمين كلمة السر
// الحل: 5 محاولات فاشلة / دقيقة / IP — بعدها 429 Too Many Requests
const authAttempts = new Map(); // IP → {count, resetAt}

function checkAuthRateLimit(ip) {
  const now = Date.now();
  let a = authAttempts.get(ip);
  if (!a || now > a.resetAt) {
    a = { count: 0, resetAt: now + 60_000 }; // نافذة دقيقة واحدة
  }
  a.count++;
  authAttempts.set(ip, a);
  return a.count > 5; // حد 5 محاولات
}

// تنظيف authAttempts كل 5 دقائق لمنع memory leak
setInterval(() => {
  const now = Date.now();
  for (const [ip, a] of authAttempts.entries()) {
    if (now > a.resetAt) authAttempts.delete(ip);
  }
}, 5 * 60_000);

app.post('/api/host-auth', (req, res) => {
  const { pass } = req.body || {};
  const clientIp = req.ip || 'unknown';

  // تحقق من rate limit أولاً — قبل أي مقارنة
  if (checkAuthRateLimit(clientIp)) {
    log.warn('host-auth: rate limit exceeded', { ip: clientIp });
    return res.status(429).json({ ok: false, error: 'محاولات كثيرة — انتظر دقيقة وحاول مرة أخرى' });
  }

  if (!pass || pass !== ADMIN_PASS) {
    log.warn('host-auth: failed attempt', { ip: clientIp });
    return res.status(401).json({ ok: false, error: 'كلمة السر غلط' });
  }
  // [02] MEDIUM: crypto.randomBytes بدلاً من Math.random
  // Math.random() ليس cryptographically secure في V8 — قابل للتنبؤ نظرياً
  // crypto.randomBytes مدمج في Node.js بدون أي package إضافي
  const { randomBytes } = require('crypto');
  const token = randomBytes(32).toString('hex'); // 64-char hex — آمن تماماً
  hostSessions.add(token);
  // نظّف بعد 8 ساعات (كافي لأي فعالية)
  setTimeout(() => hostSessions.delete(token), 8 * 60 * 60 * 1000);
  log.info('host-auth: success', { ip: req.ip });
  res.json({ ok: true, token });
});

// middleware للتحقق من token المضيف
function requireHostToken(req, res, next) {
  const token = req.headers['x-host-token'] || req.query.token || '';
  if (!token || !hostSessions.has(token)) {
    return res.status(401).json({ error: 'Unauthorized — سجّل دخول أولاً' });
  }
  next();
}

// ── AI Key — محمي بـ host token ──
app.get('/ai-key', requireHostToken, (req, res) => {
  const key = process.env.COHERE_API_KEY || '';
  if (!key) return res.status(404).json({ error: 'COHERE_API_KEY not set' });
  res.json({ key });
});

// ── QR ──
app.get('/qr', async (req, res) => {
  const url = req.query.url || '';
  if (!url) return res.status(400).send('missing url');
  try {
    const svg = await QRCode.toString(url, {
      type: 'svg', width: 200, margin: 2,
      color: { dark: '#111111', light: '#ffffff' },
    });
    res.setHeader('Content-Type', 'image/svg+xml');
    res.setHeader('Cache-Control', 'public, max-age=3600');
    res.send(svg);
  } catch (e) {
    log.error('/qr error', { message: e.message });
    res.status(500).send('QR error');
  }
});

// ══════════════════════════════════════════
//  DATA
// ══════════════════════════════════════════
let rooms     = {};
let dotsRooms = {};

const TEAM_COLORS = [
  { name:'أحمر',    color:'#ef4444', emoji:'🔴' },
  { name:'أزرق',    color:'#3b82f6', emoji:'🔵' },
  { name:'أخضر',    color:'#22c55e', emoji:'🟢' },
  { name:'أصفر',    color:'#eab308', emoji:'🟡' },
  { name:'بنفسجي',  color:'#a855f7', emoji:'🟣' },
  { name:'برتقالي', color:'#f97316', emoji:'🟠' },
  { name:'وردي',    color:'#ec4899', emoji:'🩷' },
  { name:'فيروزي',  color:'#06b6d4', emoji:'🩵' },
  { name:'بيج',     color:'#d97706', emoji:'🟤' },
  { name:'رمادي',   color:'#6b7280', emoji:'⚫' },
];

function generateCode() {
  // [12] LOW: تحقق من عدم التكرار قبل إعادة الكود
  // مع 50 غرفة نشطة، احتمال التصادم منخفض لكن ليس صفراً
  let code;
  let attempts = 0;
  do {
    code = Math.floor(100000 + Math.random() * 900000).toString();
    attempts++;
    if (attempts > 100) {
      log.error('generateCode: could not find unique code after 100 attempts');
      break;
    }
  } while (rooms[code] || dotsRooms[code]);
  return code;
}

// getPlayerList — لا يُرسل _disconnectTimer للكلاينت
function getPlayerList(room) {
  return Object.entries(room.players).map(([sid, p]) => ({
    socketId: sid,
    name:     p.name,
    team:     p.team  || '',
    score:    p.score,
    streak:   p.streak || 0,
  }));
}

function shuffleArray(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function getRoomsSnapshot() {
  return Object.entries(rooms).map(([code, room]) => ({
    code,
    title:       room.quiz?.title || 'بدون عنوان',
    state:       room.state,
    gameMode:    room.gameMode || 'solo',
    playerCount: Object.keys(room.players).length,
    players:     getPlayerList(room),
    currentQ:    room.currentQ,
    totalQ:      room.quiz?.questions?.length || 0,
  }));
}

// ── Quiz Validator ──
function validateQuiz(quiz) {
  if (!quiz || typeof quiz !== 'object')        return 'quiz مفقود';
  if (!Array.isArray(quiz.questions))           return 'quiz.questions يجب أن تكون مصفوفة';
  if (quiz.questions.length === 0)              return 'يجب أن يحتوي الكويز على سؤال واحد على الأقل';
  for (let i = 0; i < quiz.questions.length; i++) {
    const q = quiz.questions[i];
    if (!q.question || typeof q.question !== 'string') return `السؤال ${i+1}: question مفقود`;
    if (!Array.isArray(q.answers) || q.answers.length < 2) return `السؤال ${i+1}: يجب أن يكون هناك خياران على الأقل`;
    if (typeof q.correct !== 'number' || q.correct < 0 || q.correct >= q.answers.length)
      return `السؤال ${i+1}: correct غير صالح`;
  }
  return null; // صالح
}

// ══════════════════════════════════════════
//  GAME LOGIC
// ══════════════════════════════════════════
function nextQuestion(code) {
  const room = rooms[code];
  if (!room) return log.warn('nextQuestion: room not found', { code });

  room.currentQ++;
  if (room.currentQ >= room.quiz.questions.length) return endGame(code);

  room.state             = 'question';
  room.questionStartTime = Date.now();

  const q     = room.quiz.questions[room.currentQ];
  const total = room.quiz.questions.length;

  log.event('question:start', { code, q: room.currentQ + 1, total });

  io.to(room.host).emit('host:question', {
    index: room.currentQ, total,
    question: q.question, answers: q.answers,
    correct: q.correct, time: q.time, image: q.image || null,
  });

  io.to(code).emit('game:question', {
    index: room.currentQ, total,
    question: q.question, answers: q.answers,
    time: q.time, image: q.image || null,
    doublePoints: q.doublePoints || false,
  });
}

function showResults(code) {
  const room = rooms[code];
  if (!room) return log.warn('showResults: room not found', { code });

  const qIdx      = room.currentQ;
  const answerList = room.answerTimes[qIdx] || [];
  const sorted    = [...answerList].sort((a, b) => a.elapsed - b.elapsed);

  // speedBonus — idempotent
  sorted.slice(0, 5).forEach(entry => {
    const player = room.players[entry.socketId];
    if (!player || player.answers[qIdx] == null) return;
    if (player.answers[qIdx].speedBonus !== 0) return; // لا تُضاف مرتين
    const dp    = room.quiz.questions[qIdx].doublePoints ? 2 : 1;
    const bonus = 20 * dp;
    player.answers[qIdx].speedBonus = bonus;
    player.answers[qIdx].points    += bonus;
    player.score                   += bonus;
  });

  room.state = 'leaderboard';

  // ── تحرير الذاكرة — answerTimes للسؤال المنتهي لم تعد مطلوبة ──
  // مع 120 لاعب × 30 سؤال = آلاف الكائنات تتراكم بلا فائدة
  delete room.answerTimes[qIdx];

  const q   = room.quiz.questions[qIdx];
  const stats = q.answers.map((_, i) => ({
    count: Object.values(room.players).filter(
      p => p.answers[qIdx]?.answerIndex === i
    ).length,
  }));

  const leaderboard = Object.values(room.players)
    .sort((a, b) => b.score - a.score).slice(0, 10)
    .map(p => ({ name: p.name, team: p.team || '', score: p.score, streak: p.streak || 0 }));

  // أرسل لكل لاعب نتيجته الخاصة
  Object.entries(room.players).forEach(([sid, player]) => {
    const ans  = player.answers[qIdx];
    const rank = Object.values(room.players)
      .sort((a, b) => b.score - a.score)
      .findIndex(p => p === player) + 1;
    io.to(sid).emit('player:answerResult', {
      correct:    ans ? ans.correct : false,
      points:     ans ? ans.points  : 0,
      totalScore: player.score,
      rank,
    });
  });

  io.to(room.host).emit('host:results', {
    correct: q.correct, stats, leaderboard, answers: q.answers,
    isLast: qIdx + 1 >= room.quiz.questions.length,
  });

  io.to(code).emit('game:results', {
    stats, leaderboard, answers: q.answers,
    isLast: qIdx + 1 >= room.quiz.questions.length,
  });

  log.event('question:results', { code, q: qIdx + 1, players: Object.keys(room.players).length });
}

function endGame(code) {
  const room = rooms[code];
  if (!room) return log.warn('endGame: room not found', { code });

  room.state = 'finished';

  const final = Object.values(room.players)
    .sort((a, b) => b.score - a.score)
    .map((p, i) => ({ rank: i+1, name: p.name, team: p.team||'', score: p.score, maxStreak: p.maxStreak||0 }));

  let teamScores = null;
  if (room.gameMode === 'team' && room.teams?.length) {
    teamScores = {};
    room.teams.forEach(t => { teamScores[t.name] = { score:0, color:t.color, emoji:t.emoji }; });
    Object.values(room.players).forEach(p => {
      if (p.team && teamScores[p.team]) teamScores[p.team].score += p.score;
    });
  }

  io.to(code).emit('game:end', { final, teamScores, prizes: room.prizes || '' });
  io.to('display:' + code).emit('display:end', { final });
  io.to('admins').emit('admin:rooms', getRoomsSnapshot());

  log.event('game:end', { code, players: final.length, winner: final[0]?.name });

  setTimeout(() => {
    delete rooms[code];
    log.info('room:deleted', { code });
  }, 10 * 60 * 1000);
}

function advanceGame(code) {
  const room = rooms[code];
  if (!room) return;
  // [13] LOW: تنظيف answerTimes عند التخطي
  // host:skipQuestion يستدعي advanceGame مباشرة بدون مرور بـ showResults
  // showResults تحذف answerTimes لكن skipQuestion يتجاوزها — تتراكم في الذاكرة
  if (room.currentQ >= 0) delete room.answerTimes[room.currentQ];
  if (room.currentQ + 1 >= room.quiz.questions.length) endGame(code);
  else nextQuestion(code);
}

// ══════════════════════════════════════════
//  DOTS LOGIC
// ══════════════════════════════════════════
function getDotsPlayerList(room) {
  return Object.entries(room.players).map(([sid, p]) => ({
    socketId: sid, name: p.name, team: p.team, score: p.score,
  }));
}

function checkBoxes(room, lineKey) {
  const n     = room.gridSize;
  const parts = lineKey.split('_');
  const type = parts[0], r = +parts[1], c = +parts[2];
  const newBoxes = [];
  const check = (br, bc) => {
    if (br < 0 || br >= n-1 || bc < 0 || bc >= n-1) return;
    if (room.lines[`h_${br}_${bc}`] && room.lines[`h_${br+1}_${bc}`] &&
        room.lines[`v_${br}_${bc}`] && room.lines[`v_${br}_${bc+1}`]) {
      const key = `${br}_${bc}`;
      if (!room.boxes[key]) { room.boxes[key] = room.currentTurn; newBoxes.push(key); }
    }
  };
  if (type === 'h') { check(r-1, c); check(r, c); }
  else              { check(r, c-1); check(r, c); }
  return newBoxes;
}

function calcScores(room) {
  const scores = { red:0, blue:0 };
  Object.values(room.boxes).forEach(t => { if (t === 'red' || t === 'blue') scores[t]++; });
  return scores;
}

function isGameComplete(room) {
  return Object.keys(room.boxes).length >= (room.gridSize - 1) ** 2;
}

function dotsNextQuestion(code) {
  const room = dotsRooms[code];
  if (!room) return;
  room.currentQ++;
  if (room.currentQ >= room.questions.length) { dotsEndGame(code); return; }
  room.state       = 'question';
  room.answerTimes = {};
  const q     = room.questions[room.currentQ];
  const total = room.questions.length;
  io.to('dots_'+code).emit('dots:question', { index:room.currentQ, total, question:q.question, answers:q.answers, time:q.time });
  io.to(room.host).emit('dots:question',    { index:room.currentQ, total, question:q.question, answers:q.answers, correct:q.correct, time:q.time });
}

function dotsShowResults(code) {
  const room = dotsRooms[code];
  if (!room) return;
  room.state    = 'results';
  const q       = room.questions[room.currentQ];
  let winPlayer = null, winTime = Infinity;
  Object.entries(room.players).forEach(([sid, player]) => {
    const ans = player.answers[room.currentQ];
    if (ans?.correct && ans.answerTime < winTime) { winTime = ans.answerTime; winPlayer = { sid, player }; }
  });
  const winTeam = winPlayer?.player.team || null;
  const winName = winPlayer?.player.name || null;
  Object.entries(room.players).forEach(([sid, player]) => {
    const ans = player.answers[room.currentQ];
    io.to(sid).emit('dots:answerResult', { correct: ans?.correct||false, points: ans?.points||0 });
  });
  const scores      = calcScores(room);
  const leaderboard = Object.values(room.players).sort((a,b)=>b.score-a.score).slice(0,5)
    .map(p=>({ name:p.name, team:p.team, score:p.score }));
  const canDrawLine = !!winTeam && !isGameComplete(room);
  room.pendingWinTeam      = winTeam;
  room.pendingWinPlayerSid = winPlayer?.sid;
  io.to('dots_'+code).emit('dots:results', { correct:q.correct, winTeam, winName, scores, leaderboard, canDrawLine });
  io.to(room.host).emit('dots:results',    { correct:q.correct, winTeam, winName, scores, leaderboard, canDrawLine });
  if (canDrawLine && winPlayer) io.to(winPlayer.sid).emit('dots:canDrawLine', { team:winTeam, gridSize:room.gridSize });
}

function dotsEndGame(code) {
  const room = dotsRooms[code];
  if (!room) return;
  room.state     = 'finished';
  const scores   = calcScores(room);
  const winner   = scores.red > scores.blue ? 'red' : scores.blue > scores.red ? 'blue' : 'tie';
  io.to('dots_'+code).emit('dots:gameEnd', { winner, scores });
  setTimeout(() => { delete dotsRooms[code]; }, 10 * 60 * 1000);
}

// ══════════════════════════════════════════
//  SOCKET WRAPPER — try-catch + flood protection
// ══════════════════════════════════════════

// حد أقصى للـ events من socket واحد في الثانية
// يمنع client مُصاب أو لاعب يضغط بسرعة جنونية من تعطيل السيرفر
const FLOOD_LIMIT  = 20;   // events/ثانية per socket
const FLOOD_WINDOW = 1000; // ms

// flood state يُخزن على الـ socket object مباشرة
function checkFlood(socket, eventName) {
  const now = Date.now();
  if (!socket._flood) socket._flood = { count: 0, window: now };

  if (now - socket._flood.window > FLOOD_WINDOW) {
    socket._flood.count  = 0;
    socket._flood.window = now;
  }
  socket._flood.count++;

  if (socket._flood.count > FLOOD_LIMIT) {
    log.warn('socket:flood', { id: socket.id, event: eventName, count: socket._flood.count });
    return true; // مُفعَّل الـ flood
  }
  return false;
}

function safeHandler(socket, eventName, fn) {
  return (data) => {
    if (checkFlood(socket, eventName)) return;
    try {
      fn(data || {});
    } catch (err) {
      log.error(`socket handler crash: ${eventName}`, {
        message: err.message,
        stack:   err.stack?.split('\n')[1]?.trim(),
      });
    }
  };
}

// ══════════════════════════════════════════
//  SOCKET EVENTS
// ══════════════════════════════════════════
io.on('connection', socket => {
  log.info('socket:connected', { id: socket.id });

  // ── Host ──
  socket.on('host:create', safeHandler(socket, 'host:create', ({ quiz, gameMode, teamNames }) => {
    // Validate quiz
    const validationError = validateQuiz(quiz);
    if (validationError) {
      log.warn('host:create invalid quiz', { error: validationError, socketId: socket.id });
      return socket.emit('error', `كويز غير صالح: ${validationError}`);
    }
    if (Object.keys(rooms).length >= MAX_ROOMS) {
      log.warn('host:create max rooms reached', { max: MAX_ROOMS });
      return socket.emit('error', 'السيرفر مشغول — حاول لاحقاً');
    }
    const code  = generateCode();
    const teams = (teamNames||[]).map((name, i) => ({
      name:  name || TEAM_COLORS[i]?.name || ('فريق '+(i+1)),
      color: TEAM_COLORS[i]?.color || '#fff',
      emoji: TEAM_COLORS[i]?.emoji || '⚪',
    }));
    rooms[code] = {
      host: socket.id, players: {}, quiz,
      gameMode:  gameMode || 'solo', teams,
      state:     'lobby', currentQ: -1, answerTimes: {},
      survivor:  quiz.survivor  || false,
      prizes:    quiz.prizes    || '',
      paused:    false,
    };
    socket.join(code);
    socket.join('host:' + code);
    socket.data.hostCode = code;
    socket.emit('host:created', { code, mode: gameMode||'solo', teams });
    io.to('admins').emit('admin:rooms', getRoomsSnapshot());
    log.event('room:created', { code, mode: gameMode, questions: quiz.questions.length });
  }));

  // ── Display Screen ──
  socket.on('display:join', safeHandler(socket, 'display:join', ({ code }) => {
    const room = rooms[code];
    if (!room) {
      log.warn('display:join room not found', { code });
      return socket.emit('display:error', 'الغرفة غير موجودة');
    }
    socket.join('display:' + code);
    socket.data.displayCode = code;

    const currentState = { code, title: room.quiz.title, state: room.state };

    if (room.state === 'question') {
      const q = room.quiz.questions[room.currentQ];
      // نُرسل elapsed حتى تبدأ شاشة العرض التايمر من الوقت المتبقي الحقيقي
      // مثلاً: السؤال وقته 20 ثانية، مضت 8 → display تبدأ من 12
      const elapsedSec = Math.floor((Date.now() - (room.questionStartTime || Date.now())) / 1000);
      currentState.questionData = {
        index: room.currentQ, total: room.quiz.questions.length,
        question: q.question, answers: q.answers,
        time: q.time, image: q.image || null,
        elapsed: elapsedSec,
      };
    } else if (room.state === 'leaderboard') {
      const q      = room.quiz.questions[room.currentQ];
      const stats  = q.answers.map((_, i) => ({
        count: Object.values(room.players).filter(p => p.answers[room.currentQ]?.answerIndex === i).length,
      }));
      const leaderboard = Object.values(room.players)
        .sort((a,b) => b.score - a.score).slice(0,10)
        .map(p => ({ name:p.name, team:p.team||'', score:p.score, streak:p.streak||0 }));
      currentState.resultsData = { correct: q.correct, stats, leaderboard, answers: q.answers };
    } else if (room.state === 'finished') {
      const final = Object.values(room.players)
        .sort((a,b) => b.score - a.score)
        .map((p,i) => ({ rank:i+1, name:p.name, team:p.team||'', score:p.score }));
      currentState.finalData = { final };
    }

    socket.emit('display:joined', currentState);
    log.event('display:joined', { code, state: room.state });
  }));

  // ── Reactions ──
  socket.on('player:reaction', safeHandler(socket, 'player:reaction', ({ code, emoji }) => {
    const room   = rooms[code];
    if (!room || room.state === 'lobby' || room.state === 'finished') return;
    const player = room.players[socket.id];
    if (!player) return;
    const allowed = ['🔥','😂','😮','👏'];
    if (!allowed.includes(emoji)) return;
    io.to('display:'+code).emit('display:reaction', { emoji, name: player.name });
    io.to(room.host).emit('host:reaction', { emoji, name: player.name });
  }));

  // ── Pause / Resume ──
  socket.on('host:pause', safeHandler(socket, 'host:pause', ({ code }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    room.paused = true;
    io.to(code).emit('game:paused');
    io.to('display:'+code).emit('display:sync', { action: 'paused' });
    log.event('game:paused', { code });
  }));

  socket.on('host:resume', safeHandler(socket, 'host:resume', ({ code }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    room.paused = false;
    io.to(code).emit('game:resumed');
    io.to('display:'+code).emit('display:sync', { action: 'resumed' });
    log.event('game:resumed', { code });
  }));

  socket.on('host:syncDisplay', safeHandler(socket, 'host:syncDisplay', ({ code, action, data }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    io.to('display:'+code).emit('display:sync', { action, data });
  }));

  socket.on('host:start', safeHandler(socket, 'host:start', ({ code }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    if (room.state !== 'lobby') {
      log.warn('host:start called on non-lobby room', { code, state: room.state });
      return;
    }
    log.event('game:start', { code, players: Object.keys(room.players).length });
    nextQuestion(code);
    io.to('admins').emit('admin:rooms', getRoomsSnapshot());
  }));

  socket.on('host:showResults', safeHandler(socket, 'host:showResults', ({ code }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    showResults(code);
  }));

  socket.on('host:next',         safeHandler(socket, 'host:next',         ({ code }) => { const r=rooms[code]; if(!r||r.host!==socket.id) return; advanceGame(code); }));
  socket.on('host:endGame',      safeHandler(socket, 'host:endGame',      ({ code }) => { const r=rooms[code]; if(!r||r.host!==socket.id) return; endGame(code); }));
  socket.on('host:skipQuestion', safeHandler(socket, 'host:skipQuestion', ({ code }) => { const r=rooms[code]; if(!r||r.host!==socket.id) return; advanceGame(code); }));

  socket.on('host:renamePlayer', safeHandler(socket, 'host:renamePlayer', ({ code, socketId, newName }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    if (!room.players[socketId]) return;
    room.players[socketId].name = newName;
    io.to(socketId).emit('player:renamed', { newName });
    io.to(room.host).emit('host:playerList', { players: getPlayerList(room) });
  }));

  socket.on('host:kickPlayer', safeHandler(socket, 'host:kickPlayer', ({ code, socketId }) => {
    const room = rooms[code];
    if (!room || room.host !== socket.id) return;
    if (!room.players[socketId]) return;
    io.to(socketId).emit('player:kicked');
    delete room.players[socketId];
    io.to(room.host).emit('host:playerList', { players: getPlayerList(room) });
    io.to(code).emit('room:update', { players: getPlayerList(room) });
  }));

  // ── Player ──
  socket.on('player:join', safeHandler(socket, 'player:join', ({ code, name, team, playerId }) => {
    if (!code || !name) return socket.emit('error', 'بيانات ناقصة');
    const room = rooms[code];
    if (!room) return socket.emit('error', 'الكود غير صحيح');

    // إعادة اتصال
    if (playerId) {
      const existing   = Object.values(room.players).find(p => p.playerId === playerId);
      if (existing) {
        // [R1] ألغِ timer الحذف
        if (existing._disconnectTimer) {
          clearTimeout(existing._disconnectTimer);
          existing._disconnectTimer = null;
        }
        const oldSocketId = Object.keys(room.players).find(k => room.players[k].playerId === playerId);
        if (oldSocketId && oldSocketId !== socket.id) {
          room.players[socket.id] = room.players[oldSocketId];
          delete room.players[oldSocketId];
        }
        socket.join(code);
        socket.data.code     = code;
        socket.data.playerId = playerId;
        socket.emit('player:rejoined', { name: existing.name, score: existing.score, state: room.state });
        if (room.state === 'question') {
          const q = room.quiz.questions[room.currentQ];
          // [11] LOW: أرسل elapsed حتى اللاعع العائد يرى الوقت المتبقي الحقيقي
          // بدونه: شريط الوقت يبدأ من 20 ثانية حتى لو مضت 15 منها
          const rejoinElapsed = Math.floor((Date.now() - (room.questionStartTime || Date.now())) / 1000);
          socket.emit('game:question', {
            index: room.currentQ, total: room.quiz.questions.length,
            question: q.question, answers: q.answers,
            time: q.time, image: q.image || null,
            elapsed: rejoinElapsed,
          });
        }
        io.to(room.host).emit('host:playerList', { players: getPlayerList(room) });
        log.event('player:rejoined', { code, name: existing.name });
        return;
      }
    }

    if (room.state !== 'lobby') return socket.emit('error', 'اللعبة بدأت بالفعل');
    const nameTaken = Object.values(room.players).some(
      p => p.name.trim().toLowerCase() === name.trim().toLowerCase()
    );
    if (nameTaken) return socket.emit('error', 'هذا الاسم مأخوذ — اختر اسماً آخر');

    const newPlayerId   = playerId || (Date.now().toString(36) + Math.random().toString(36).slice(2));
    room.players[socket.id] = {
      name, team: team||'', score: 0, answers: {},
      playerId: newPlayerId, streak: 0, maxStreak: 0,
    };
    socket.join(code);
    socket.data.code     = code;
    socket.data.playerId = newPlayerId;
    socket.emit('player:joined', { name, team: team||'', playerId: newPlayerId });
    if (room.gameMode === 'team' && room.teams) socket.emit('room:teams', { teams: room.teams });
    io.to(room.host).emit('host:playerList', { players: getPlayerList(room) });
    io.to(code).emit('room:update', { players: getPlayerList(room) });
    io.to('admins').emit('admin:rooms', getRoomsSnapshot());
    log.event('player:joined', { code, name });
  }));

  socket.on('player:answer', safeHandler(socket, 'player:answer', ({ code, answerIndex }) => {
    const room = rooms[code];
    // [R4] قبل فقط في state === 'question'
    if (!room || room.state !== 'question') return;
    const player = room.players[socket.id];
    if (!player || player.answers[room.currentQ] != null) return;
    if (player.eliminated) return;

    const q = room.quiz.questions[room.currentQ];
    if (typeof answerIndex !== 'number' || !Number.isInteger(answerIndex) ||
        answerIndex < 0 || answerIndex >= q.answers.length) return;

    const correct = typeof q.correct === 'number' && answerIndex === q.correct;
    const elapsed = Date.now() - (room.questionStartTime || Date.now());
    if (elapsed < 400) return; // anti-bot

    if (!room.answerTimes[room.currentQ]) room.answerTimes[room.currentQ] = [];
    room.answerTimes[room.currentQ].push({ name: player.name, elapsed, correct, socketId: socket.id });

    if (correct) {
      player.streak    = (player.streak    || 0) + 1;
      player.maxStreak = Math.max(player.maxStreak || 0, player.streak);
    } else {
      player.streak = 0;
    }

    const streakBonus   = (correct && player.streak >= 3) ? 10 : 0;
    const dp            = q.doublePoints ? 2 : 1;
    const correctPoints = correct ? 50 * dp : 0;
    player.score       += correctPoints + streakBonus;
    player.answers[room.currentQ] = {
      answerIndex, correct,
      points: correctPoints + streakBonus,
      correctPoints, speedBonus: 0, elapsed, streakBonus,
    };

    socket.emit('player:answered', { streak: player.streak, streakBonus, doublePoints: dp === 2 });

    if (room.survivor && !correct) {
      player.eliminated = true;
      socket.emit('player:eliminated', { reason: 'إجابة خاطئة' });
    }

    const answeredCount = Object.values(room.players).filter(
      p => p.answers[room.currentQ] != null
    ).length;
    io.to(room.host).emit('host:answeredCount', {
      count: answeredCount, total: Object.keys(room.players).length,
    });
  }));

  // ── Dots ──
  socket.on('dots:create', safeHandler(socket, 'dots:create', ({ questions, gridSize }) => {
    if (!Array.isArray(questions) || questions.length === 0)
      return socket.emit('error', 'أسئلة غير صالحة');
    const code = generateCode();
    dotsRooms[code] = {
      host: socket.id, players: {}, questions, gridSize: gridSize||5,
      state: 'lobby', currentQ: -1, lines: {}, boxes: {}, answerTimes: {},
      pendingWinTeam: null,
    };
    socket.join('dots_'+code);
    socket.emit('dots:created', { code });
  }));

  socket.on('dots:join', safeHandler(socket, 'dots:join', ({ code, name, team }) => {
    const room = dotsRooms[code];
    if (!room) return socket.emit('error', 'الكود غير صحيح');
    if (room.state !== 'lobby') return socket.emit('error', 'اللعبة بدأت');
    if (Object.values(room.players).filter(p=>p.team===team).length >= 2)
      return socket.emit('error', 'الفريق ممتلئ!');
    room.players[socket.id] = { name, team, score:0, answers:[] };
    socket.join('dots_'+code);
    socket.data.dotsCode = code;
    socket.emit('dots:joined', { name, team, gridSize: room.gridSize });
    const playerList = getDotsPlayerList(room);
    io.to(room.host).emit('dots:playerList', { players: playerList });
    io.to('dots_'+code).emit('dots:playerList', { players: playerList });
  }));

  socket.on('dots:kick',        safeHandler(socket, 'dots:kick',        ({ code, socketId })  => { const r=dotsRooms[code]; if(!r||r.host!==socket.id) return; io.to(socketId).emit('dots:kicked'); delete r.players[socketId]; io.to(r.host).emit('dots:playerList',{players:getDotsPlayerList(r)}); }));
  socket.on('dots:start',       safeHandler(socket, 'dots:start',       ({ code })            => { const r=dotsRooms[code]; if(!r||r.host!==socket.id) return; dotsNextQuestion(code); }));
  socket.on('dots:showResults', safeHandler(socket, 'dots:showResults', ({ code })            => { const r=dotsRooms[code]; if(!r||r.host!==socket.id) return; dotsShowResults(code); }));
  socket.on('dots:skip',        safeHandler(socket, 'dots:skip',        ({ code })            => { const r=dotsRooms[code]; if(!r||r.host!==socket.id) return; dotsNextQuestion(code); }));
  socket.on('dots:next',        safeHandler(socket, 'dots:next',        ({ code })            => { const r=dotsRooms[code]; if(!r||r.host!==socket.id) return; if(isGameComplete(r)) dotsEndGame(code); else dotsNextQuestion(code); }));

  socket.on('dots:answer', safeHandler(socket, 'dots:answer', ({ code, answerIndex }) => {
    const room = dotsRooms[code];
    if (!room || room.state !== 'question') return;
    const player = room.players[socket.id];
    if (!player || player.answers[room.currentQ] !== undefined) return;
    const q       = room.questions[room.currentQ];
    const correct = answerIndex === q.correct;
    const now     = Date.now();
    player.answers[room.currentQ] = { answerIndex, correct, answerTime: now };
    if (!room.answerTimes[room.currentQ]) room.answerTimes[room.currentQ] = [];
    room.answerTimes[room.currentQ].push({ name: player.name, time: now, correct });
    const suspicious = room.answerTimes[room.currentQ].filter(a => Math.abs(a.time - now) < 1000);
    if (suspicious.length >= 3)
      io.to(room.host).emit('dots:suspicious', { message: `⚠️ ${suspicious.length} لاعبين أجابوا في نفس الوقت!` });
    const count   = Object.values(room.players).filter(p => p.answers[room.currentQ] !== undefined).length;
    const barData = q.answers.map((_, i) => Object.values(room.players).filter(p => p.answers[room.currentQ]?.answerIndex === i).length);
    io.to(room.host).emit('dots:answeredCount', { count, total: Object.keys(room.players).length, barData });
  }));

  socket.on('dots:drawLine', safeHandler(socket, 'dots:drawLine', ({ code, lineKey }) => {
    const room = dotsRooms[code];
    if (!room) return;
    const player = room.players[socket.id];
    if (!player || room.lines[lineKey] || socket.id !== room.pendingWinPlayerSid) return;
    room.currentTurn  = player.team;
    room.lines[lineKey] = player.team;
    const newBoxes = checkBoxes(room, lineKey);
    const scores   = calcScores(room);
    io.to('dots_'+code).emit('dots:lineDrawn', { lineKey, team: player.team, newBoxes, scores });
    io.to(room.host).emit('dots:lineDrawn',    { lineKey, team: player.team, newBoxes, scores });
    room.pendingWinTeam = null;
  }));

  // ── Admin ──
  socket.on('admin:subscribe', safeHandler(socket, 'admin:subscribe', ({ pass }) => {
    if (pass !== ADMIN_PASS) return socket.emit('admin:error', 'كلمة السر غلط');
    socket.join('admins');
    socket.emit('admin:rooms', getRoomsSnapshot());
  }));

  socket.on('admin:getRooms', safeHandler(socket, 'admin:getRooms', ({ pass }) => {
    if (pass !== ADMIN_PASS) return socket.emit('admin:error', 'كلمة السر غلط');
    socket.emit('admin:rooms', getRoomsSnapshot());
  }));

  socket.on('admin:kickPlayer', safeHandler(socket, 'admin:kickPlayer', ({ pass, code, socketId }) => {
    if (pass !== ADMIN_PASS) return;
    const room = rooms[code];
    if (!room || !room.players[socketId]) return;
    io.to(socketId).emit('player:kicked');
    delete room.players[socketId];
    io.to(room.host).emit('host:playerList', { players: getPlayerList(room) });
    io.to(code).emit('room:update', { players: getPlayerList(room) });
    socket.emit('admin:rooms', getRoomsSnapshot());
  }));

  socket.on('admin:closeRoom', safeHandler(socket, 'admin:closeRoom', ({ pass, code }) => {
    if (pass !== ADMIN_PASS) return;
    const room = rooms[code];
    if (!room) return;
    io.to(code).emit('player:kicked');
    delete rooms[code];
    socket.emit('admin:rooms', getRoomsSnapshot());
    log.event('room:force-closed', { code });
  }));

  // ── Host Rejoin ──
  socket.on('host:rejoin', safeHandler(socket, 'host:rejoin', ({ code, token }) => {
    // [04] MEDIUM: تحقق من token قبل قبول host:rejoin
    // بدون هذا: أي شخص يعرف الـ roomCode يستطيع إرسال host:rejoin ويسيطر على الغرفة
    if (!token || !hostSessions.has(token)) {
      log.warn('host:rejoin: invalid token', { code, id: socket.id });
      return socket.emit('error', 'انتهت جلستك — سجّل دخول مرة أخرى');
    }
    const room = rooms[code];
    if (!room) return;
    room.host = socket.id;
    socket.join(code);
    socket.join('host:'+code);
    socket.data.hostCode = code;
    socket.emit('host:playerList',  { players: getPlayerList(room) });
    socket.emit('host:reconnected', { code, state: room.state });
    log.event('host:rejoined', { code });
  }));

  // ── Disconnect ──
  socket.on('disconnect', safeHandler(socket, 'disconnect', () => {
    log.info('socket:disconnected', { id: socket.id });

    // Dots cleanup
    const dotsCode = socket.data.dotsCode;
    if (dotsCode && dotsRooms[dotsCode]?.host === socket.id) {
      setTimeout(() => { delete dotsRooms[dotsCode]; }, 30_000);
    }

    // Quiz player grace period
    const code = socket.data.code;
    if (!code || !rooms[code] || !rooms[code].players[socket.id]) return;

    const player = rooms[code].players[socket.id];
    player._disconnectTimer = setTimeout(() => {
      const room = rooms[code];
      if (!room) return;
      const stillExists = room.players[socket.id];
      if (stillExists?.playerId === player.playerId) {
        delete room.players[socket.id];
        io.to(code).emit('room:update', { players: getPlayerList(room) });
        if (room.host) io.to(room.host).emit('host:playerList', { players: getPlayerList(room) });
        io.to('admins').emit('admin:rooms', getRoomsSnapshot());
        log.event('player:removed-after-grace', { code, name: player.name });
      }
    }, GRACE_MS);
  }));
});

// ══════════════════════════════════════════
//  START SERVER
// ══════════════════════════════════════════
server.on('error', (err) => {
  log.error('server error', { message: err.message, code: err.code });
  if (err.code === 'EADDRINUSE') {
    log.error(`Port ${PORT} is already in use`);
    process.exit(1);
  }
});

server.listen(PORT, () => {
  log.info('server:started', { port: PORT, node: process.version, env: process.env.NODE_ENV || 'development' });
});
