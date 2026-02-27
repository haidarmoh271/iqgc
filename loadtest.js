// ── Load Test Script ──
// يحاكي لاعبين وهميين على السيرفر
// الاستخدام: node loadtest.js [عدد اللاعبين] [كود الغرفة]
// مثال:    node loadtest.js 50 ABC123

const { io } = require('socket.io-client');

const NUM_PLAYERS = parseInt(process.argv[2]) || 20;
const ROOM_CODE   = process.argv[3] || '';
const SERVER_URL  = process.argv[4] || 'https://alpha-quiz-production.up.railway.app';

if(!ROOM_CODE){
  console.log('❌ لازم تحدد كود الغرفة');
  console.log('الاستخدام: node loadtest.js [عدد] [كود]');
  console.log('مثال:      node loadtest.js 50 ABC123');
  process.exit(1);
}

const ARABIC_NAMES = ['أحمد','محمد','علي','خالد','فهد','سعد','عمر','يوسف','ناصر','عبدالله',
  'سارة','نورة','منى','هند','ريم','لولو','دانة','غلا','شيخة','جواهر'];

let connected = 0, answered = 0, errors = 0;
const players = [];

console.log(`\n🚀 بدء اختبار الحمل على ${SERVER_URL}`);
console.log(`👥 عدد اللاعبين: ${NUM_PLAYERS}`);
console.log(`🔑 كود الغرفة: ${ROOM_CODE}\n`);

for(let i = 0; i < NUM_PLAYERS; i++){
  setTimeout(()=>{
    const name = ARABIC_NAMES[i % ARABIC_NAMES.length] + '_' + (i+1);
    const socket = io(SERVER_URL, { transports: ['websocket'] });

    socket.on('connect', ()=>{
      connected++;
      process.stdout.write(`\r✅ متصل: ${connected}/${NUM_PLAYERS} | أجاب: ${answered} | خطأ: ${errors}`);
      // انضم للغرفة
      socket.emit('player:join', { code: ROOM_CODE, name });
    });

    socket.on('game:question', ({ answers, time })=>{
      // انتظر وقت عشوائي ثم أجب
      const delay = Math.random() * (time * 800);
      setTimeout(()=>{
        const answerIdx = Math.floor(Math.random() * answers.length);
        socket.emit('player:answer', { code: ROOM_CODE, answer: answerIdx });
        answered++;
        process.stdout.write(`\r✅ متصل: ${connected}/${NUM_PLAYERS} | أجاب: ${answered} | خطأ: ${errors}`);
      }, delay);
    });

    socket.on('player:kicked', ()=> socket.disconnect());
    socket.on('game:end', ()=> socket.disconnect());
    socket.on('connect_error', (e)=>{
      errors++;
      process.stdout.write(`\r✅ متصل: ${connected}/${NUM_PLAYERS} | أجاب: ${answered} | خطأ: ${errors}`);
    });

    players.push(socket);
  }, i * 100); // فاصل 100ms بين كل لاعب
}

// تقرير كل 5 ثواني
setInterval(()=>{
  const mem = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`\n📊 [${new Date().toLocaleTimeString()}] متصل: ${connected} | أجاب: ${answered} | خطأ: ${errors} | RAM محلي: ${mem.toFixed(1)}MB`);
}, 5000);

// إيقاف بعد 3 دقائق
setTimeout(()=>{
  console.log('\n\n⏹ انتهى الاختبار — جاري قطع الاتصال...');
  players.forEach(s => s.disconnect());
  setTimeout(()=> process.exit(0), 1000);
}, 3 * 60 * 1000);
