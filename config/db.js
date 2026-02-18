// config/db.js
const mysql = require('mysql2');
const mysqlPromise = require('mysql2/promise');

// 1. Config สำหรับ Source (Database ต้นทาง)
const sourceConfig = {
    host: 'localhost',
    user: 'root',
    password: '', 
    database: 'db_source', // แก้ชื่อ DB ตรงนี้
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// 2. Config สำหรับ Destination (Database ปลายทาง)
const destConfig = {
    host: 'localhost',
    user: 'root',
    password: '', 
    database: 'db_dest',   // แก้ชื่อ DB ตรงนี้
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// 3. สร้าง Connection Pool
// pool สำหรับ Stream (ใช้ mysql2 ธรรมดา)
const sourcePool = mysql.createPool(sourceConfig);

// pool สำหรับ Insert (ใช้ mysql2/promise เพื่อรองรับ async/await)
const destPool = mysqlPromise.createPool(destConfig);

console.log("🔌 Database Pools Created...");

module.exports = { sourcePool, destPool };