// server.js
const express = require('express');
const fs = require('fs');
const path = require('path');
const syncModel = require('./models/syncModel'); // เรียกใช้ Model

const app = express();
const PORT = 3000;

// Config ไฟล์ระบบ (Lock & State)
const LOCK_FILE = path.join(__dirname, 'sync.lock');
const STATE_FILE = path.join(__dirname, 'sync_state.json');

// ================= API ROUTE =================
app.get('/api/start-sync', (req, res) => {
    
    // 1. Check Lock (ป้องกันกดซ้ำ)
    if (fs.existsSync(LOCK_FILE)) {
        // (Logic เช็คเวลาไฟล์เก่า เหมือนเดิม... ละไว้เพื่อให้โค้ดสั้นลง)
        return res.status(429).json({ status: 'busy', message: '⛔ ระบบกำลังทำงานอยู่...' });
    }

    // 2. Create Lock
    fs.writeFileSync(LOCK_FILE, new Date().toISOString());

    // 3. Response ทันที (Fire and Forget)
    res.json({ status: 'success', message: '✅ รับคำสั่งแล้ว! ระบบกำลังทำงานเบื้องหลัง...' });

    // 4. Start Background Process
    runSyncProcess();
});

// ================= BACKGROUND LOGIC =================
async function runSyncProcess() {
    console.log("🚀 Starting Sync Process...");
    let sourceConnection = null;

    try {
        // [A] อ่าน State ล่าสุด (Resume Logic)
        let lastId = 0;
        if (fs.existsSync(STATE_FILE)) {
            const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
            lastId = state.last_id || 0;
        }

        // [B] เรียกใช้ Model เพื่อขอ Stream
        const { stream, connection } = await syncModel.getSourceStream(lastId);
        sourceConnection = connection; // เก็บไว้ release ตอนจบ

        let batchBuffer = [];
        const BATCH_SIZE = 1000;
        let totalProcessed = 0;

        // [C] จัดการ Stream Events
        stream.on('data', async (row) => {
            // เตรียมข้อมูลใส่ถัง
            batchBuffer.push([row.id, row.name, row.email, row.created_at]);

            if (batchBuffer.length >= BATCH_SIZE) {
                stream.pause(); // หยุดน้ำ
                
                const currentLastId = row.id;
                await processBatch(batchBuffer, currentLastId); // บันทึก
                
                batchBuffer = []; // ล้างถัง
                totalProcessed += BATCH_SIZE;
                stream.resume(); // เปิดน้ำต่อ
            }
        });

        stream.on('end', async () => {
            // เก็บตกก้อนสุดท้าย
            if (batchBuffer.length > 0) {
                const lastRow = batchBuffer[batchBuffer.length - 1];
                await processBatch(batchBuffer, lastRow[0]); // ID ตัวสุดท้าย
                totalProcessed += batchBuffer.length;
            }
            
            console.log(`\n✅ Finished! Total Processed: ${totalProcessed}`);
            cleanup();
        });

        stream.on('error', (err) => {
            console.error("Stream Error:", err);
            cleanup();
        });

    } catch (err) {
        console.error("System Error:", err);
        cleanup();
    } finally {
        // อย่าลืมคืน connection ของ Source
        if (sourceConnection) sourceConnection.release();
    }
}

// ฟังก์ชันย่อยสำหรับเรียก Model Save และบันทึก State
async function processBatch(data, lastId) {
    await syncModel.saveBatch(data); // สั่งบันทึกลง DB
    
    // บันทึก State ลงไฟล์
    fs.writeFileSync(STATE_FILE, JSON.stringify({ last_id: lastId }));
    process.stdout.write(`\r💾 Saved -> Last ID: ${lastId}`);
}

function cleanup() {
    if (fs.existsSync(LOCK_FILE)) fs.unlinkSync(LOCK_FILE); // ปลดล็อค
    console.log("\n🔓 System Unlocked.");
}

app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});