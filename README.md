# 🎮 QuizBlast — نسخة Railway (أونلاين)

## 🚀 رفع على Railway

### 1. ارفع على GitHub
```bash
git init
git add .
git commit -m "QuizBlast initial"
git remote add origin https://github.com/username/quizblast.git
git push -u origin main
```

### 2. ربط بـ Railway
- اذهب إلى [railway.app](https://railway.app)
- اضغط **New Project** ← **Deploy from GitHub**
- اختر المشروع
- Railway يشغّله تلقائياً ويعطيك رابط مثل:
  ```
  https://quizblast-production.up.railway.app
  ```

### 3. الاستخدام
- **المضيف:** يفتح الرابط ← ينشئ مسابقة ← يشارك QR
- **اللاعبون:** يمسحون QR أو يذهبون إلى `/play.html`

## 🏠 تشغيل محلي
```bash
npm install
npm start
# http://localhost:3000
```

## ✨ مميزات QR
- **LAN:** QR يتولد تلقائياً بـ IP جهازك
- **Railway:** QR يتولد برابط Railway مباشرة
- **ngrok:** ألصق رابط ngrok وتولد QR فوراً
