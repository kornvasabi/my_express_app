// models/syncModel.js
const { sourcePool, destPool } = require('../config/db');

module.exports = {
    // ฟังก์ชัน 1: ขอ Stream ข้อมูลจาก Source
    getSourceStream: (lastId) => {
        return new Promise((resolve, reject) => {
            sourcePool.getConnection((err, connection) => {
                if (err) return reject(err);

                console.log(`🔍 Querying from ID > ${lastId}`);
                
                // สร้าง Query Stream
                const sql = 'SELECT * FROM users WHERE id > ? ORDER BY id ASC';
                const query = connection.query(sql, [lastId]);
                
                // ส่ง Stream และ Connection กลับไปให้ไฟล์หลักจัดการ
                // (ต้องส่ง Connection ไปด้วย เพื่อให้ไฟล์หลักสั่ง release ได้เมื่อจบงาน)
                resolve({ stream: query.stream(), connection });
            });
        });
    },

    // ฟังก์ชัน 2: บันทึกข้อมูลทีละก้อน (Batch Insert + Transaction)
    saveBatch: async (dataBatch) => {
        const connection = await destPool.getConnection();
        try {
            // เริ่ม Transaction (มัดรวม)
            await connection.beginTransaction();

            // คำสั่ง SQL ยัดข้อมูล
            const sql = 'INSERT IGNORE INTO users_backup (id, name, email, created_at) VALUES ?';
            await connection.query(sql, [dataBatch]);

            // ยืนยันการบันทึก
            await connection.commit();
            return true;

        } catch (error) {
            // ถ้าพัง ให้ยกเลิกทั้งหมดในรอบนี้
            await connection.rollback();
            console.error("❌ Model Error (Rollback):", error.message);
            throw error; // ส่ง Error กลับไปให้ไฟล์หลักรู้
        } finally {
            connection.release(); // คืน Connection เข้า Pool
        }
    }
};