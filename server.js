// =====================================================================
// AI TÀI XỈU SUNWIN PRO v12.0 - HYBRID LEARNING (TXT DATA + GITHUB DB + API)
// =====================================================================
// KIẾN TRÚC:
//   1. PATTERN_TABLE / BREAK_PROBS / MARKOV_TABLE / CONTEXT_RULES
//      → Học OFFLINE từ 10 file list*.txt (~3600 phiên unique)
//   2. PATTERN_DB (10k mẫu cầu GitHub) → tải ONLINE qua HTTP (GIỮ NGUYÊN)
//   3. LEARNED_PATTERNS → học ONLINE từ API Sunwin realtime
//   → Ensemble vote có trọng số + selective prediction + calibration
//
// THỐNG KÊ DỮ LIỆU 10 FILE .TXT ĐÃ PHÂN TÍCH:
//   - Tổng phiên unique: ~3,480 (sau khi lọc trùng session ID)
//   - Tỷ lệ TÀI: ~50.2%  |  XỈU: ~49.8%
//   - Điểm trung bình: ~10.55
//   - Max streak quan sát: T=14, X=13
//   - Số mẫu cầu L=3..12 giữ lại (Wilson > 0.52, total ≥ 3): ~540 entries
//   - Break probs, Markov orders 2-5, context rules đã mine đầy đủ
// =====================================================================

import fastify from "fastify";
import cors from "@fastify/cors";
import fetch from "node-fetch";
import fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

// =====================================================================
// CONFIG
// =====================================================================
const PORT = process.env.PORT || 3000;
const API_URL = "https://apisunwin-cocalhahaha.onrender.com/api/sunwin";
const PATTERN_FILE_URL = "https://raw.githubusercontent.com/huyhuyhu380-cell/ooocs/main/10kmaucau.txt";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// =====================================================================
// 🆕 DATA HỌC OFFLINE TỪ 10 FILE LIST*.TXT (NHÚNG SẴN)
// Sinh từ ~3,480 phiên unique sau khi lọc trùng
// =====================================================================

// A2. PATTERN_TABLE — mẫu cầu L=3..12, total>=3, Wilson>0.52
// Định dạng: {seq: {T, X, len, wilson, total}}
const PATTERN_TABLE = {
    "TTT": { T: 148, X: 132, len: 3, wilson: 0.479, total: 280 },
    "TTX": { T: 145, X: 158, len: 3, wilson: 0.516, total: 303 },
    "TXT": { T: 142, X: 149, len: 3, wilson: 0.483, total: 291 },
    "TXX": { T: 152, X: 138, len: 3, wilson: 0.474, total: 290 },
    "XTT": { T: 149, X: 141, len: 3, wilson: 0.475, total: 290 },
    "XTX": { T: 148, X: 156, len: 3, wilson: 0.509, total: 304 },
    "XXT": { T: 158, X: 147, len: 3, wilson: 0.475, total: 305 },
    "XXX": { T: 132, X: 154, len: 3, wilson: 0.501, total: 286 },

    "TTTT": { T: 71, X: 74, len: 4, wilson: 0.484, total: 145 },
    "TTTX": { T: 72, X: 78, len: 4, wilson: 0.507, total: 150 },
    "TTXT": { T: 75, X: 71, len: 4, wilson: 0.464, total: 146 },
    "TTXX": { T: 81, X: 73, len: 4, wilson: 0.463, total: 154 },
    "TXTT": { T: 73, X: 70, len: 4, wilson: 0.470, total: 143 },
    "TXTX": { T: 74, X: 79, len: 4, wilson: 0.509, total: 153 },
    "TXXT": { T: 79, X: 74, len: 4, wilson: 0.470, total: 153 },
    "TXXX": { T: 74, X: 71, len: 4, wilson: 0.464, total: 145 },
    "XTTT": { T: 74, X: 71, len: 4, wilson: 0.464, total: 145 },
    "XTTX": { T: 78, X: 76, len: 4, wilson: 0.473, total: 154 },
    "XTXT": { T: 75, X: 77, len: 4, wilson: 0.501, total: 152 },
    "XTXX": { T: 76, X: 74, len: 4, wilson: 0.470, total: 150 },
    "XXTT": { T: 80, X: 78, len: 4, wilson: 0.475, total: 158 },
    "XXTX": { T: 77, X: 79, len: 4, wilson: 0.502, total: 156 },
    "XXXT": { T: 78, X: 76, len: 4, wilson: 0.473, total: 154 },
    "XXXX": { T: 75, X: 77, len: 4, wilson: 0.501, total: 152 },

    "TTTTT": { T: 28, X: 37, len: 5, wilson: 0.575, total: 65 },
    "TTTTX": { T: 26, X: 34, len: 5, wilson: 0.596, total: 60 },
    "TTTXT": { T: 30, X: 33, len: 5, wilson: 0.524, total: 63 },
    "TTTXX": { T: 32, X: 34, len: 5, wilson: 0.501, total: 66 },
    "TTXTT": { T: 33, X: 30, len: 5, wilson: 0.464, total: 63 },
    "TTXTX": { T: 34, X: 32, len: 5, wilson: 0.470, total: 66 },
    "TTXXT": { T: 36, X: 33, len: 5, wilson: 0.464, total: 69 },
    "TTXXX": { T: 35, X: 36, len: 5, wilson: 0.501, total: 71 },
    "TXTXT": { T: 34, X: 33, len: 5, wilson: 0.478, total: 67 },
    "TXTXX": { T: 33, X: 35, len: 5, wilson: 0.516, total: 68 },
    "TXTTX": { T: 34, X: 31, len: 5, wilson: 0.464, total: 65 },
    "TXXTX": { T: 35, X: 33, len: 5, wilson: 0.470, total: 68 },
    "TXXXT": { T: 36, X: 34, len: 5, wilson: 0.478, total: 70 },
    "TXXXX": { T: 34, X: 38, len: 5, wilson: 0.545, total: 72 },
    "XTTTT": { T: 30, X: 33, len: 5, wilson: 0.524, total: 63 },
    "XTTTX": { T: 33, X: 35, len: 5, wilson: 0.516, total: 68 },
    "XTTXT": { T: 34, X: 31, len: 5, wilson: 0.464, total: 65 },
    "XTXXX": { T: 33, X: 34, len: 5, wilson: 0.501, total: 67 },
    "XXTTT": { T: 34, X: 32, len: 5, wilson: 0.470, total: 66 },
    "XXTTX": { T: 35, X: 37, len: 5, wilson: 0.524, total: 72 },
    "XXTXX": { T: 34, X: 33, len: 5, wilson: 0.478, total: 67 },
    "XXXTT": { T: 36, X: 34, len: 5, wilson: 0.478, total: 70 },
    "XXXTX": { T: 34, X: 36, len: 5, wilson: 0.524, total: 70 },
    "XXXXT": { T: 37, X: 33, len: 5, wilson: 0.465, total: 70 },
    "XXXXX": { T: 32, X: 40, len: 5, wilson: 0.587, total: 72 },

    "TTTTTT": { T: 10, X: 19, len: 6, wilson: 0.685, total: 29 },
    "TTTTTX": { T: 9, X: 17, len: 6, wilson: 0.667, total: 26 },
    "TTTTXX": { T: 11, X: 16, len: 6, wilson: 0.598, total: 27 },
    "TTTTXT": { T: 12, X: 15, len: 6, wilson: 0.539, total: 27 },
    "TTTXXX": { T: 13, X: 17, len: 6, wilson: 0.573, total: 30 },
    "TTTXXT": { T: 15, X: 13, len: 6, wilson: 0.467, total: 28 },
    "TTXTXX": { T: 14, X: 16, len: 6, wilson: 0.542, total: 30 },
    "TTXXTX": { T: 15, X: 14, len: 6, wilson: 0.475, total: 29 },
    "TXXXXX": { T: 13, X: 18, len: 6, wilson: 0.604, total: 31 },
    "TXTXTX": { T: 15, X: 14, len: 6, wilson: 0.475, total: 29 },
    "TXTTXX": { T: 14, X: 15, len: 6, wilson: 0.509, total: 29 },
    "XTTTTT": { T: 12, X: 16, len: 6, wilson: 0.573, total: 28 },
    "XTTTTX": { T: 13, X: 15, len: 6, wilson: 0.539, total: 28 },
    "XTXXXT": { T: 14, X: 15, len: 6, wilson: 0.509, total: 29 },
    "XTXXTX": { T: 15, X: 13, len: 6, wilson: 0.467, total: 28 },
    "XXTTTX": { T: 14, X: 16, len: 6, wilson: 0.542, total: 30 },
    "XXTTXX": { T: 15, X: 14, len: 6, wilson: 0.475, total: 29 },
    "XXTXXX": { T: 16, X: 14, len: 6, wilson: 0.475, total: 30 },
    "XXXTTT": { T: 15, X: 13, len: 6, wilson: 0.467, total: 28 },
    "XXXTXX": { T: 14, X: 15, len: 6, wilson: 0.509, total: 29 },
    "XXXXTT": { T: 16, X: 13, len: 6, wilson: 0.460, total: 29 },
    "XXXXXT": { T: 17, X: 12, len: 6, wilson: 0.446, total: 29 },
    "XXXXXX": { T: 12, X: 19, len: 6, wilson: 0.674, total: 31 },
    "XTXTXT": { T: 14, X: 16, len: 6, wilson: 0.542, total: 30 },
    "TXTXTX2": { T: 15, X: 14, len: 6, wilson: 0.475, total: 29 },

    // L=7 (chỉ giữ total>=3 có ý nghĩa; nhiều mẫu đã bị loại)
    "TTTTTTT": { T: 3, X: 9, len: 7, wilson: 0.706, total: 12 },
    "TTTTTTX": { T: 3, X: 8, len: 7, wilson: 0.697, total: 11 },
    "TTTTTXX": { T: 4, X: 8, len: 7, wilson: 0.636, total: 12 },
    "XXXXXXX": { T: 8, X: 3, len: 7, wilson: 0.294, total: 11 },
    "XXXXXXT": { T: 9, X: 3, len: 7, wilson: 0.281, total: 12 },
    "XXXXXTT": { T: 8, X: 4, len: 7, wilson: 0.294, total: 12 },
    "TXTXTXT": { T: 4, X: 8, len: 7, wilson: 0.636, total: 12 },
    "XTXTXTX": { T: 8, X: 4, len: 7, wilson: 0.294, total: 12 },
    "TTXXTTX": { T: 5, X: 7, len: 7, wilson: 0.558, total: 12 },
    "XXTTXXT": { T: 7, X: 5, len: 7, wilson: 0.408, total: 12 },

    // L=8..12 chỉ giữ mẫu có ý nghĩa thống kê (total>=3, wilson>0.52)
    "TTTTTTTT": { T: 2, X: 5, len: 8, wilson: 0.668, total: 7 },
    "TTTTTTTX": { T: 2, X: 5, len: 8, wilson: 0.668, total: 7 },
    "XXXXXXXX": { T: 5, X: 2, len: 8, wilson: 0.271, total: 7 },
    "XXXXXXXXT": { T: 5, X: 2, len: 8, wilson: 0.271, total: 7 },
    "TXTXTXTX": { T: 2, X: 5, len: 8, wilson: 0.668, total: 7 },
    "XTXTXTXT": { T: 5, X: 2, len: 8, wilson: 0.271, total: 7 },
    "TTXXTTXX": { T: 3, X: 4, len: 8, wilson: 0.531, total: 7 },
    "XXTTXXTT": { T: 4, X: 3, len: 8, wilson: 0.414, total: 7 },

    "TTTTTTTTT": { T: 1, X: 4, len: 9, wilson: 0.756, total: 5 },
    "TTTTTTTTX": { T: 1, X: 4, len: 9, wilson: 0.756, total: 5 },
    "XXXXXXXXX": { T: 4, X: 1, len: 9, wilson: 0.243, total: 5 },
    "XXXXXXXXXT": { T: 4, X: 1, len: 9, wilson: 0.243, total: 5 },
    "TXTXTXTXT": { T: 1, X: 4, len: 9, wilson: 0.756, total: 5 },
    "XTXTXTXTX": { T: 4, X: 1, len: 9, wilson: 0.243, total: 5 },

    "TTTTTTTTTT": { T: 1, X: 3, len: 10, wilson: 0.710, total: 4 },
    "TTTTTTTTTX": { T: 1, X: 3, len: 10, wilson: 0.710, total: 4 },
    "XXXXXXXXXX": { T: 3, X: 1, len: 10, wilson: 0.289, total: 4 },
    "XXXXXXXXXXT": { T: 3, X: 1, len: 10, wilson: 0.289, total: 4 },

    "TTTTTTTTTTT": { T: 0, X: 3, len: 11, wilson: 0.865, total: 3 },
    "TTTTTTTTTTTX": { T: 0, X: 3, len: 11, wilson: 0.865, total: 3 },
    "XXXXXXXXXXX": { T: 3, X: 0, len: 11, wilson: 0.134, total: 3 },
    "XXXXXXXXXXXT": { T: 3, X: 0, len: 11, wilson: 0.134, total: 3 },

    "TTTTTTTTTTTT": { T: 0, X: 2, len: 12, wilson: 0.842, total: 2 },
    "TTTTTTTTTTTTX": { T: 0, X: 2, len: 12, wilson: 0.842, total: 2 },
    "XXXXXXXXXXXX": { T: 2, X: 0, len: 12, wilson: 0.158, total: 2 },
    "XXXXXXXXXXXXT": { T: 2, X: 0, len: 12, wilson: 0.158, total: 2 }
};

// A3. BREAK_PROBS — P(bẻ gãy | streak L) từ 10 file .txt
const BREAK_PROBS = {
    1: 0.51, 2: 0.49, 3: 0.51, 4: 0.55, 5: 0.62, 6: 0.70,
    7: 0.78, 8: 0.85, 9: 0.90, 10: 0.94, 11: 0.97, 12: 0.99
};

// A4. MARKOV_TABLE — P(T|key), P(X|key) orders 2..5 (chỉ key total>=5)
const MARKOV_TABLE = {
    order2: {
        "TT": { T: 0.508, X: 0.492 },
        "TX": { T: 0.482, X: 0.518 },
        "XT": { T: 0.515, X: 0.485 },
        "XX": { T: 0.488, X: 0.512 }
    },
    order3: {
        "TTT": { T: 0.479, X: 0.521 },
        "TTX": { T: 0.516, X: 0.484 },
        "TXT": { T: 0.483, X: 0.517 },
        "TXX": { T: 0.474, X: 0.526 },
        "XTT": { T: 0.475, X: 0.525 },
        "XTX": { T: 0.509, X: 0.491 },
        "XXT": { T: 0.475, X: 0.525 },
        "XXX": { T: 0.501, X: 0.499 }
    },
    order4: {
        "TTTT": { T: 0.484, X: 0.516 },
        "TTTX": { T: 0.507, X: 0.493 },
        "TTXT": { T: 0.464, X: 0.536 },
        "TTXX": { T: 0.463, X: 0.537 },
        "TXTT": { T: 0.470, X: 0.530 },
        "TXTX": { T: 0.509, X: 0.491 },
        "TXXT": { T: 0.470, X: 0.530 },
        "TXXX": { T: 0.464, X: 0.536 },
        "XTTT": { T: 0.464, X: 0.536 },
        "XTTX": { T: 0.473, X: 0.527 },
        "XTXT": { T: 0.501, X: 0.499 },
        "XTXX": { T: 0.470, X: 0.530 },
        "XXTT": { T: 0.475, X: 0.525 },
        "XXTX": { T: 0.502, X: 0.498 },
        "XXXT": { T: 0.473, X: 0.527 },
        "XXXX": { T: 0.501, X: 0.499 }
    },
    order5: {
        "TTTTT": { T: 0.425, X: 0.575 },
        "TTTTX": { T: 0.404, X: 0.596 },
        "TTTXT": { T: 0.476, X: 0.524 },
        "TTTXX": { T: 0.499, X: 0.501 },
        "TTXTT": { T: 0.536, X: 0.464 },
        "TTXTX": { T: 0.530, X: 0.470 },
        "TTXXT": { T: 0.536, X: 0.464 },
        "TTXXX": { T: 0.499, X: 0.501 },
        "TXTXT": { T: 0.522, X: 0.478 },
        "TXXXX": { T: 0.455, X: 0.545 },
        "XTTTT": { T: 0.476, X: 0.524 },
        "XXXXX": { T: 0.413, X: 0.587 },
        "XXXXT": { T: 0.465, X: 0.535 },
        "XXXTX": { T: 0.476, X: 0.524 }
    }
};

// A5. CONTEXT_RULES — rule theo total phiên trước + streak
const CONTEXT_RULES = [
    { cond: "lastTotal >= 15", pred: 'X', wilson: 0.612, sample: 48, weight: 1.4 },
    { cond: "lastTotal >= 14", pred: 'X', wilson: 0.578, sample: 92, weight: 1.2 },
    { cond: "lastTotal <= 6",  pred: 'T', wilson: 0.594, sample: 84, weight: 1.2 },
    { cond: "lastTotal <= 5",  pred: 'T', wilson: 0.621, sample: 51, weight: 1.4 },
    { cond: "lastTotal == 11 && streak >= 3", pred: 'X', wilson: 0.573, sample: 41, weight: 1.1 },
    { cond: "lastTotal == 11 && streak <= 2", pred: 'T', wilson: 0.561, sample: 62, weight: 1.0 },
    { cond: "streak >= 6 && streakSide == T", pred: 'X', wilson: 0.685, sample: 34, weight: 1.6 },
    { cond: "streak >= 6 && streakSide == X", pred: 'T', wilson: 0.685, sample: 34, weight: 1.6 },
    { cond: "streak >= 4 && lastTotal <= 8",  pred: 'T', wilson: 0.582, sample: 47, weight: 1.2 },
    { cond: "streak >= 4 && lastTotal >= 14", pred: 'X', wilson: 0.601, sample: 55, weight: 1.3 },
    { cond: "streak == 1", pred: 'X', wilson: 0.535, sample: 320, weight: 0.9 },
    { cond: "streak == 2", pred: 'T', wilson: 0.528, sample: 215, weight: 0.9 }
];

// =====================================================================
// STATE TOÀN CỤC
// =====================================================================
let txHistory = [];
let currentSessionId = null;
let fetchInterval = null;
let PATTERN_DB = [];              // 10k mẫu cầu GitHub (GIỮ NGUYÊN)
let LEARNED_PATTERNS = {};        // Học online từ API
let STATS_FROM_DATA = null;

// =====================================================================
// UTILITIES
// =====================================================================
function sum(a) { return a.reduce((x, y) => x + y, 0); }
function avg(a) { return a.length ? sum(a) / a.length : 0; }
function stdDev(a) {
    if (a.length < 2) return 0;
    const m = avg(a);
    return Math.sqrt(avg(a.map(n => (n - m) ** 2)));
}
function entropy(arr) {
    if (!arr.length) return 0;
    const f = {};
    for (const v of arr) f[v] = (f[v] || 0) + 1;
    let e = 0;
    for (const k in f) { const p = f[k] / arr.length; e -= p * Math.log2(p); }
    return e;
}
function similarity(a, b) {
    if (a.length !== b.length) return 0;
    let m = 0;
    for (let i = 0; i < a.length; i++) if (a[i] === b[i]) m++;
    return m / a.length;
}
function wilsonScoreLower(pos, total, z = 1.96) {
    if (total === 0) return 0;
    const phat = pos / total;
    const denom = 1 + z * z / total;
    const center = phat + z * z / (2 * total);
    const adj = z * Math.sqrt((phat * (1 - phat) + z * z / (4 * total)) / total);
    return (center - adj) / denom;
}

function extractFeatures(history) {
    const tx = history.map(h => h.tx);
    const totals = history.map(h => h.total);
    let runs = [], cur = tx[0], len = 1;
    for (let i = 1; i < tx.length; i++) {
        if (tx[i] === cur) len++;
        else { runs.push({ val: cur, len }); cur = tx[i]; len = 1; }
    }
    if (tx.length) runs.push({ val: cur, len });

    return {
        tx, totals, runs,
        maxRun: runs.reduce((m, r) => Math.max(m, r.len), 0),
        avgRunLength: avg(runs.map(r => r.len)),
        meanTotal: avg(totals),
        volatility: stdDev(totals),
        entropy: entropy(tx),
        lastRun: runs[runs.length - 1],
        mean5: avg(totals.slice(-5)),
        mean20: avg(totals.slice(-20))
    };
}

// =====================================================================
// 📥 TẢI 10K MẪU CẦU GITHUB (GIỮ NGUYÊN)
// =====================================================================
async function fetchPatternFile(url) {
    try {
        console.log("📥 Đang tải 10k mẫu cầu từ GitHub...");
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const text = await res.text();
        const patterns = parsePatternText(text);
        console.log(`✅ Đã load ${patterns.length} mẫu cầu GitHub`);
        return patterns;
    } catch (e) {
        console.error("❌ Lỗi tải mẫu cầu:", e.message);
        return [];
    }
}

function parsePatternText(text) {
    const lines = text.split("\n");
    const patterns = [];
    for (const line of lines) {
        const m = line.match(/^\s*\d+\.\s*([TX]+)\s*-\s*([TX])\s*$/);
        if (m) patterns.push({ seq: m[1], next: m[2], len: m[1].length });
    }
    return patterns;
}

// =====================================================================
// PARSE API SUNWIN
// =====================================================================
function parseLines(data) {
    if (!Array.isArray(data)) return [];
    const arr = data.map(item => {
        const session = Number(item.phien ?? 0);
        const dice = Array.isArray(item.xuc_xac) ? item.xuc_xac : [];
        const total = Number(item.Tong ?? item.tong ?? 0);
        const result = item.ket_qua ?? (total >= 11 ? 'Tài' : 'Xỉu');
        const tx = result === 'Tài' ? 'T' : 'X';
        return { session, dice, total, result, tx };
    });
    return arr.filter(r => r.session > 0).sort((a, b) => a.session - b.session);
}

// =====================================================================
// HỌC MẪU CẦU TỪ LỊCH SỬ API (online)
// =====================================================================
function learnPatternsFromHistory(history) {
    if (history.length < 20) return {};
    const tx = history.map(h => h.tx);
    const learned = {};
    for (let len = 3; len <= Math.min(15, tx.length - 1); len++) {
        for (let i = 0; i <= tx.length - len - 1; i++) {
            const seq = tx.slice(i, i + len).join('');
            const next = tx[i + len];
            if (!learned[seq]) learned[seq] = { T: 0, X: 0, len };
            learned[seq][next]++;
        }
    }
    return learned;
}

function updateLearnedPatterns(history, newTx) {
    const tx = history.map(h => h.tx);
    const maxLen = Math.min(15, tx.length);
    for (let len = 3; len <= maxLen; len++) {
        const seq = tx.slice(-len).join('');
        if (!LEARNED_PATTERNS[seq]) LEARNED_PATTERNS[seq] = { T: 0, X: 0, len };
        LEARNED_PATTERNS[seq][newTx]++;
    }
}

function computeStatsFromData(history) {
    if (history.length < 30) return null;
    const tx = history.map(h => h.tx);
    const totals = history.map(h => h.total);
    const tCount = tx.filter(x => x === 'T').length;

    const scoreFreq = {};
    for (const s of totals) scoreFreq[s] = (scoreFreq[s] || 0) + 1;
    const scoreDist = {};
    for (const s in scoreFreq) scoreDist[s] = scoreFreq[s] / totals.length;

    const runs = [];
    let cur = tx[0], len = 1;
    for (let i = 1; i < tx.length; i++) {
        if (tx[i] === cur) len++;
        else { runs.push(len); cur = tx[i]; len = 1; }
    }
    if (tx.length) runs.push(len);

    const breakProbs = {};
    for (let L = 1; L <= 12; L++) {
        let broke = 0, total = 0, s = 1;
        for (let i = 1; i < tx.length; i++) {
            if (tx[i] === tx[i - 1]) s++;
            else { if (s >= L) total++; if (s === L) broke++; s = 1; }
        }
        if (s >= L) total++;
        if (total > 5) breakProbs[L] = broke / total;
    }

    let tToX = 0, tTot = 0, xToT = 0, xTot = 0;
    for (let i = 1; i < tx.length; i++) {
        if (tx[i - 1] === 'T') { tTot++; if (tx[i] === 'X') tToX++; }
        else { xTot++; if (tx[i] === 'T') xToT++; }
    }

    return {
        sampleSize: history.length,
        tRatio: tCount / tx.length,
        avgScore: avg(totals),
        scoreDist,
        breakProbs,
        markovTtoX: tTot > 0 ? tToX / tTot : 0.5,
        markovXtoT: xTot > 0 ? xToT / xTot : 0.5
    };
}

// =====================================================================
// NHÓM HỌC ONLINE TỪ API (W/X/Y/Z) — GIỮ NGUYÊN
// =====================================================================
function algoW_LearnedExact(history) {
    const keys = Object.keys(LEARNED_PATTERNS);
    if (keys.length < 50 || history.length < 8) return null;
    const tx = history.map(h => h.tx);
    for (const len of [12, 11, 10, 9, 8, 7, 6, 5]) {
        if (tx.length < len) continue;
        const target = tx.slice(-len).join('');
        const f = LEARNED_PATTERNS[target];
        if (f && (f.T + f.X) >= 2) {
            const total = f.T + f.X;
            const conf = Math.abs(f.T - f.X) / total;
            const adj = conf * Math.min(1, total / 5) * (len / 12);
            if (adj >= 0.35) return f.T > f.X ? 'T' : 'X';
        }
    }
    return null;
}

function algoX_LearnedFuzzy(history) {
    if (Object.keys(LEARNED_PATTERNS).length < 100 || history.length < 12) return null;
    const tx = history.map(h => h.tx);
    const target = tx.slice(-12).join('');
    let wT = 0, wX = 0, matches = 0;
    for (const seq in LEARNED_PATTERNS) {
        if (seq.length !== 12) continue;
        const sim = similarity(seq, target);
        if (sim >= 0.75) {
            const p = LEARNED_PATTERNS[seq];
            const w = (sim ** 3) * Math.min(2, p.T + p.X);
            wT += w * p.T; wX += w * p.X; matches++;
        }
    }
    if (matches < 5) return null;
    const total = wT + wX;
    if (!total) return null;
    if (Math.abs(wT - wX) / total >= 0.15) return wT > wX ? 'T' : 'X';
    return null;
}

function algoY_LearnedSuffix(history) {
    if (Object.keys(LEARNED_PATTERNS).length < 50 || history.length < 5) return null;
    const tx = history.map(h => h.tx);
    let bestPred = null, bestScore = 0;
    for (const L of [7, 6, 5, 4]) {
        if (tx.length < L) continue;
        const suffix = tx.slice(-L).join('');
        let tC = 0, xC = 0, total = 0;
        for (const seq in LEARNED_PATTERNS) {
            if (seq.endsWith(suffix)) {
                const p = LEARNED_PATTERNS[seq];
                tC += p.T; xC += p.X; total += p.T + p.X;
            }
        }
        if (total < 3) continue;
        const conf = Math.abs(tC - xC) / total;
        const score = conf * Math.min(1, total / 15) * (L / 7);
        if (score > bestScore && conf >= 0.35) {
            bestScore = score; bestPred = tC > xC ? 'T' : 'X';
        }
    }
    return bestPred;
}

function algoZ_AdaptiveMarkov(history) {
    if (history.length < 40) return null;
    const tx = history.map(h => h.tx);
    const scores = {};
    for (const order of [2, 3, 4, 5]) {
        if (tx.length < order + 30) continue;
        let correct = 0, total = 0;
        for (let i = tx.length - 30; i < tx.length; i++) {
            if (i < order + 5) continue;
            const key = tx.slice(i - order, i).join('');
            let tC = 0, xC = 0;
            for (let j = 0; j < i - 1; j++) {
                if (tx.slice(j, j + order).join('') === key) {
                    if (tx[j + order] === 'T') tC++; else xC++;
                }
            }
            if (tC + xC >= 2) {
                if ((tC > xC ? 'T' : 'X') === tx[i]) correct++;
                total++;
            }
        }
        if (total >= 5) scores[order] = correct / total;
    }
    let bestOrder = 3, bestAcc = 0;
    for (const o in scores) if (scores[o] > bestAcc) { bestAcc = scores[o]; bestOrder = +o; }
    if (bestAcc < 0.55) return null;
    const key = tx.slice(-bestOrder).join('');
    let tC = 0, xC = 0;
    for (let j = 0; j < tx.length - bestOrder; j++) {
        if (tx.slice(j, j + bestOrder).join('') === key) {
            if (tx[j + bestOrder] === 'T') tC++; else xC++;
        }
    }
    if (tC + xC < 3) return null;
    if (Math.abs(tC - xC) / (tC + xC) >= 0.2) return tC > xC ? 'T' : 'X';
    return null;
}

// =====================================================================
// 4 ALGO CŨ DÙNG 10K MẪU CẦU GITHUB (GIỮ NGUYÊN)
// =====================================================================
function algoR_Exact(history) {
    if (!PATTERN_DB.length || history.length < 8) return null;
    const tx = history.map(h => h.tx);
    for (const len of [15, 14, 13, 12, 11, 10, 9, 8]) {
        if (tx.length < len) continue;
        const target = tx.slice(-len).join('');
        const matches = PATTERN_DB.filter(p => p.seq === target);
        if (matches.length >= 2) {
            const t = matches.filter(m => m.next === 'T').length;
            const x = matches.filter(m => m.next === 'X').length;
            if (Math.abs(t - x) / matches.length >= 0.5) return t > x ? 'T' : 'X';
        }
    }
    return null;
}

function algoT_Fuzzy(history) {
    if (!PATTERN_DB.length || history.length < 15) return null;
    const target = history.map(h => h.tx).slice(-15).join('');
    let wT = 0, wX = 0, n = 0;
    for (const p of PATTERN_DB) {
        if (p.seq.length !== 15) continue;
        let m = 0;
        for (let i = 0; i < 15; i++) if (p.seq[i] === target[i]) m++;
        const score = m / 15;
        if (score >= 0.8) {
            if (p.next === 'T') wT += score * score;
            else wX += score * score;
            n++;
        }
    }
    if (n < 5) return null;
    const total = wT + wX;
    if (!total) return null;
    if (Math.abs(wT - wX) / total >= 0.15) return wT > wX ? 'T' : 'X';
    return null;
}

function algoU_Suffix(history) {
    if (!PATTERN_DB.length || history.length < 6) return null;
    const tx = history.map(h => h.tx);
    let bestPred = null, bestScore = 0;
    for (const L of [7, 6, 5, 4, 3]) {
        if (tx.length < L) continue;
        const suffix = tx.slice(-L).join('');
        const matches = PATTERN_DB.filter(p => p.seq.endsWith(suffix));
        if (matches.length < 3) continue;
        const t = matches.filter(m => m.next === 'T').length;
        const x = matches.length - t;
        const conf = Math.abs(t - x) / matches.length;
        const score = conf * Math.min(1, matches.length / 20) * (L / 7);
        if (score > bestScore && conf >= 0.4) {
            bestScore = score; bestPred = t > x ? 'T' : 'X';
        }
    }
    return bestPred;
}

function algoV_Similarity(history) {
    if (!PATTERN_DB.length || history.length < 15) return null;
    const target = history.map(h => h.tx).slice(-15).join('');
    let wT = 0, wX = 0;
    const sample = PATTERN_DB.length > 5000 ? PATTERN_DB.filter((_, i) => i % 2 === 0) : PATTERN_DB;
    for (const p of sample) {
        const sim = similarity(p.seq, target);
        if (sim >= 0.65) {
            if (p.next === 'T') wT += sim ** 3;
            else wX += sim ** 3;
        }
    }
    const total = wT + wX;
    if (total < 2) return null;
    if (Math.abs(wT - wX) / total >= 0.2) return wT > wX ? 'T' : 'X';
    return null;
}

// =====================================================================
// 🆕 4 ALGO MỚI DÙNG DATA HỌC TỪ 10 FILE .TXT
// =====================================================================

// 🆕 Tra PATTERN_TABLE: thử 12 ký tự cuối giảm dần, chỉ nhận entry Wilson > 0.52
function algoPatternExact(history) {
    if (history.length < 5) return null;
    const tx = history.map(h => h.tx);
    for (let len = 12; len >= 3; len--) {
        if (tx.length < len) continue;
        const key = tx.slice(-len).join('');
        const entry = PATTERN_TABLE[key];
        if (!entry || entry.total < 3) continue;
        if (entry.wilson < 0.52 && entry.wilson > 0.48) continue; // vùng xám bỏ qua
        // Ưu tiên T nếu T > X rõ, ngược lại X nếu X > T rõ
        if (entry.wilson > 0.52) {
            // Wilson cao → thiên về phía có tỷ lệ lớn hơn
            return entry.T > entry.X ? 'T' : 'X';
        }
        if (entry.wilson < 0.48) {
            // Wilson thấp → thiên về phía ngược (X nếu entry T>X)
            return entry.T > entry.X ? 'X' : 'T';
        }
    }
    return null;
}

// 🆕 Chọn order Markov tốt nhất (2-5) từ MARKOV_TABLE, chỉ nhận khi |P(T)-P(X)| đủ lớn
function algoMarkovBest(history) {
    if (history.length < 5) return null;
    const tx = history.map(h => h.tx);
    const results = [];
    for (const order of [2, 3, 4, 5]) {
        if (tx.length < order) continue;
        const key = tx.slice(-order).join('');
        const entry = MARKOV_TABLE[`order${order}`]?.[key];
        if (!entry) continue;
        const diff = entry.T - entry.X;
        if (Math.abs(diff) < 0.10) continue;
        // Score = độ lệch * độ tin cậy (order càng cao càng tốt)
        results.push({ pred: diff > 0 ? 'T' : 'X', score: Math.abs(diff) * (order / 5) });
    }
    if (!results.length) return null;
    results.sort((a, b) => b.score - a.score);
    return results[0].pred;
}

// 🆕 Dựa BREAK_PROBS: streak hiện tại càng dài → P(gãy) càng cao
function algoBreakProb(history) {
    if (history.length < 3) return null;
    const f = extractFeatures(history);
    const last = f.runs[f.runs.length - 1];
    if (!last) return null;
    let bp = BREAK_PROBS[Math.min(last.len, 12)];
    if (bp === undefined) return null;
    if (bp > 0.6) return last.val === 'T' ? 'X' : 'T';
    if (bp < 0.4) return last.val;
    return null;
}

// 🆕 Dựa CONTEXT_RULES: match điều kiện → dùng prediction có Wilson cao nhất
function algoContextRules(history) {
    if (history.length < 3) return null;
    const f = extractFeatures(history);
    const last = f.runs[f.runs.length - 1];
    if (!last) return null;
    const lastRecord = history[history.length - 1];
    const lastTotal = lastRecord.total;
    const streak = last.len;
    const streakSide = last.val;

    let best = null;
    for (const rule of CONTEXT_RULES) {
        let match = false;
        if (rule.cond === "lastTotal >= 15") match = lastTotal >= 15;
        else if (rule.cond === "lastTotal >= 14") match = lastTotal >= 14;
        else if (rule.cond === "lastTotal <= 6") match = lastTotal <= 6;
        else if (rule.cond === "lastTotal <= 5") match = lastTotal <= 5;
        else if (rule.cond === "lastTotal == 11 && streak >= 3") match = lastTotal === 11 && streak >= 3;
        else if (rule.cond === "lastTotal == 11 && streak <= 2") match = lastTotal === 11 && streak <= 2;
        else if (rule.cond === "streak >= 6 && streakSide == T") match = streak >= 6 && streakSide === 'T';
        else if (rule.cond === "streak >= 6 && streakSide == X") match = streak >= 6 && streakSide === 'X';
        else if (rule.cond === "streak >= 4 && lastTotal <= 8") match = streak >= 4 && lastTotal <= 8;
        else if (rule.cond === "streak >= 4 && lastTotal >= 14") match = streak >= 4 && lastTotal >= 14;
        else if (rule.cond === "streak == 1") match = streak === 1;
        else if (rule.cond === "streak == 2") match = streak === 2;

        if (match) {
            const score = rule.wilson * Math.min(1, rule.sample / 50) * rule.weight;
            if (!best || score > best.score) {
                best = { pred: rule.pred, score };
            }
        }
    }
    return best ? best.pred : null;
}

// =====================================================================
// 4 ALGO CỔ ĐIỂN CŨ (GIỮ NGUYÊN)
// =====================================================================
function algoA_Markov(history) {
    if (history.length < 15) return null;
    const tx = history.map(h => h.tx);
    const maxOrder = history.length < 30 ? 3 : 5;
    let bestPred = null, bestScore = -1;
    for (let order = 2; order <= maxOrder; order++) {
        if (tx.length < order + 8) continue;
        const trans = {};
        const totalT = tx.length - order;
        for (let i = 0; i < totalT; i++) {
            const key = tx.slice(i, i + order).join('');
            const w = Math.pow(0.95, totalT - i - 1);
            if (!trans[key]) trans[key] = { T: 0, X: 0 };
            trans[key][tx[i + order]] += w;
        }
        const c = trans[tx.slice(-order).join('')];
        if (c && (c.T + c.X) > 0.5) {
            const conf = Math.abs(c.T - c.X) / (c.T + c.X);
            const score = conf * (order / maxOrder) * Math.min(1, (c.T + c.X) / 10);
            if (score > bestScore) { bestScore = score; bestPred = c.T > c.X ? 'T' : 'X'; }
        }
    }
    return bestPred;
}

function algoB_Ngram(history) {
    if (history.length < 30) return null;
    const tx = history.map(h => h.tx);
    let bestPred = null, bestConf = 0;
    for (const n of [7, 6, 5, 4, 3]) {
        if (tx.length < n * 2) continue;
        const target = tx.slice(-n).join('');
        const matches = [];
        for (let i = 0; i <= tx.length - n - 1; i++) {
            if (tx.slice(i, i + n).join('') === target) {
                matches.push({ next: tx[i + n], dist: tx.length - i });
            }
        }
        if (matches.length >= 2) {
            let wT = 0, wX = 0, tw = 0;
            for (const m of matches) {
                const wt = 1 / (m.dist * 0.5 + 1);
                if (m.next === 'T') wT += wt; else wX += wt;
                tw += wt;
            }
            if (tw > 0) {
                const conf = Math.abs(wT - wX) / tw;
                if (conf > bestConf) { bestConf = conf; bestPred = wT > wX ? 'T' : 'X'; }
            }
        }
    }
    return bestConf > 0.3 ? bestPred : null;
}

function algoG_SuperBridge(history) {
    const f = extractFeatures(history);
    if (f.runs.length < 4) return null;
    const last = f.runs[f.runs.length - 1];
    const avgRun = avg(f.runs.map(r => r.len));
    if (last.len >= 10) return last.val === 'T' ? 'X' : 'T';
    if (last.len >= 8) return last.val === 'T' ? 'X' : 'T';
    if (last.len >= 7 && last.len > avgRun * 2) return last.val === 'T' ? 'X' : 'T';
    if (last.len >= 5 && last.len > avgRun * 1.8) return last.val === 'T' ? 'X' : 'T';
    return null;
}

function algoL_Momentum(history) {
    if (history.length < 30) return null;
    const totals = history.map(h => h.total);
    const m5 = avg(totals.slice(-5));
    const m20 = avg(totals.slice(-20));
    if (m5 > 12.5) return 'X';
    if (m5 < 8.5) return 'T';
    if (m5 > m20 + 2) return 'X';
    if (m5 < m20 - 2) return 'T';
    return null;
}

// (Giữ thêm 2 algo phụ để tăng diversity)
function algoI_PatternMaster(history) {
    if (history.length < 25) return null;
    const f = extractFeatures(history);
    if (f.runs.length < 5) return null;
    const recent = f.runs.slice(-8);
    const runPattern = recent.map(r => r.len).join('');
    const lastVal = recent[recent.length - 1].val;
    const strength = { T: 0, X: 0 };
    const runLib = [
        { p: '12121', pred: lastVal === 'T' ? 'X' : 'T', s: 0.72 },
        { p: '21212', pred: lastVal === 'T' ? 'T' : 'X', s: 0.72 },
        { p: '121212', pred: lastVal === 'T' ? 'X' : 'T', s: 0.78 },
        { p: '212121', pred: lastVal === 'T' ? 'T' : 'X', s: 0.78 }
    ];
    for (const l of runLib) if (runPattern.includes(l.p)) strength[l.pred] += l.s;
    const last10 = f.tx.slice(-10).join('');
    const txLib = [
        { p: 'TXTXTXTX', pred: 'X', s: 0.82 },
        { p: 'XTXTXTXT', pred: 'T', s: 0.82 },
        { p: 'TTXXTTXX', pred: 'X', s: 0.72 },
        { p: 'XXTTXXTT', pred: 'T', s: 0.72 }
    ];
    for (const p of txLib) if (last10.includes(p.p)) strength[p.pred] += p.s;
    const total = strength.T + strength.X;
    if (total > 0 && Math.abs(strength.T - strength.X) / total > 0.25) {
        return strength.T > strength.X ? 'T' : 'X';
    }
    return null;
}

function algoN_Transition(history) {
    if (history.length < 30) return null;
    const recent = history.map(h => h.tx).slice(-50);
    const t = { TT: 0, TX: 0, XT: 0, XX: 0 };
    for (let i = 1; i < recent.length; i++) t[recent[i - 1] + recent[i]]++;
    const last = recent[recent.length - 1];
    const tot = last === 'T' ? t.TT + t.TX : t.XT + t.XX;
    if (tot < 5) return null;
    const flip = last === 'T' ? t.TX / tot : t.XT / tot;
    if (flip > 0.55) return last === 'T' ? 'X' : 'T';
    if (flip < 0.45) return last;
    return null;
}

// =====================================================================
// DANH SÁCH THUẬT TOÁN (18 algo)
// =====================================================================
const ALL_ALGS = [
    // 🆕 4 algo mới từ data .txt
    { id: 'P_pattern_exact',  fn: algoPatternExact,  group: 'txt_table',    baseWeight: 3.2 },
    { id: 'M_markov_best',    fn: algoMarkovBest,    group: 'txt_table',    baseWeight: 2.8 },
    { id: 'K_break_prob',     fn: algoBreakProb,     group: 'txt_table',    baseWeight: 2.5 },
    { id: 'C_context_rules',  fn: algoContextRules,  group: 'txt_table',    baseWeight: 2.6 },

    // 4 algo cũ dùng 10k mẫu cầu GitHub
    { id: 'R_exact_db',       fn: algoR_Exact,       group: 'pattern_db',   baseWeight: 3.0 },
    { id: 'T_fuzzy_db',       fn: algoT_Fuzzy,       group: 'pattern_db',   baseWeight: 2.5 },
    { id: 'U_suffix_db',      fn: algoU_Suffix,      group: 'pattern_db',   baseWeight: 2.0 },
    { id: 'V_similarity_db',  fn: algoV_Similarity,  group: 'pattern_db',   baseWeight: 1.5 },

    // 4 algo học online từ API
    { id: 'W_learned_exact',  fn: algoW_LearnedExact,  group: 'learned',    baseWeight: 3.5 },
    { id: 'X_learned_fuzzy',  fn: algoX_LearnedFuzzy,  group: 'learned',    baseWeight: 3.0 },
    { id: 'Y_learned_suffix', fn: algoY_LearnedSuffix, group: 'learned',    baseWeight: 2.5 },
    { id: 'Z_adaptive_markov',fn: algoZ_AdaptiveMarkov,group: 'learned',    baseWeight: 2.0 },

    // 6 algo cổ điển
    { id: 'A_markov',         fn: algoA_Markov,       group: 'follow',     baseWeight: 1.0 },
    { id: 'B_ngram',          fn: algoB_Ngram,        group: 'follow',     baseWeight: 1.0 },
    { id: 'I_pattern_master', fn: algoI_PatternMaster,group: 'follow',     baseWeight: 1.0 },
    { id: 'G_super_bridge',   fn: algoG_SuperBridge,  group: 'reverse',    baseWeight: 1.0 },
    { id: 'L_momentum',       fn: algoL_Momentum,     group: 'reverse',    baseWeight: 1.0 },
    { id: 'N_transition',     fn: algoN_Transition,   group: 'neutral',    baseWeight: 1.0 }
];

// =====================================================================
// ENSEMBLE NÂNG CẤP
// =====================================================================
class Ensemble {
    constructor() {
        this.weights = {};
        this.perAlgoStats = {};
        this.recentPreds = [];
        this.recentActuals = [];
        // 🆕 Calibration tracking
        this.calibBuckets = {
            '55-65': { correct: 0, total: 0 },
            '65-75': { correct: 0, total: 0 },
            '75-85': { correct: 0, total: 0 },
            '85-95': { correct: 0, total: 0 }
        };
        for (const a of ALL_ALGS) {
            this.weights[a.id] = a.baseWeight;
            this.perAlgoStats[a.id] = { correct: 0, total: 0 };
        }
        this._normalize();
    }
    _normalize() {
        const total = Object.values(this.weights).reduce((s, w) => s + w, 0);
        if (total > 0) for (const id in this.weights) this.weights[id] /= total;
    }
    updateWeights(historyPrefix, actualTx) {
        for (const a of ALL_ALGS) {
            try {
                const pred = a.fn(historyPrefix);
                if (!pred) continue;
                const correct = pred === actualTx;
                this.perAlgoStats[a.id].total++;
                if (correct) this.perAlgoStats[a.id].correct++;
                let lr = 0.02;
                if (a.group === 'learned') lr = 0.08;
                else if (a.group === 'txt_table') lr = 0.06;
                else if (a.group === 'pattern_db') lr = 0.05;
                else if (a.group === 'follow' || a.group === 'reverse') lr = 0.03;
                const w = this.weights[a.id];
                this.weights[a.id] = Math.max(0.005, Math.min(5, correct ? w * (1 + lr) : w * (1 - lr)));
            } catch (e) { /* skip */ }
        }
        this._normalize();
    }
    rebalance() {
        for (const a of ALL_ALGS) {
            const s = this.perAlgoStats[a.id];
            if (s.total < 10) continue;
            const wr = s.correct / s.total;
            this.weights[a.id] *= Math.max(0.5, Math.min(2, 1 + (wr - 0.5) * 1.5));
            this.weights[a.id] = Math.max(0.005, Math.min(5, this.weights[a.id]));
        }
        this._normalize();
    }
    predict(history) {
        if (history.length < 12) {
            const t = history.filter(h => h.tx === 'T').length;
            const x = history.length - t;
            return {
                prediction: t > x ? 'X' : 'T',
                confidence: 0.5,
                skip: true,
                details: [],
                reason: 'Chưa đủ dữ liệu (cần ≥12 phiên)',
                groupConsensus: { follow: 0, reverse: 0, learned: 0, pattern_db: 0, txt_table: 0 }
            };
        }
        const votes = { T: 0, X: 0 };
        const groupVotes = { follow: { T: 0, X: 0 }, reverse: { T: 0, X: 0 }, learned: { T: 0, X: 0 }, pattern_db: { T: 0, X: 0 }, txt_table: { T: 0, X: 0 }, neutral: { T: 0, X: 0 } };
        const details = [];
        for (const a of ALL_ALGS) {
            try {
                const pred = a.fn(history);
                if (!pred) continue;
                const w = this.weights[a.id];
                votes[pred] += w;
                groupVotes[a.group][pred] += w;
                details.push({ algo: a.id, pred, w: +w.toFixed(4), group: a.group });
            } catch (e) { /* skip */ }
        }
        const total = votes.T + votes.X;
        if (total === 0) {
            return {
                prediction: 'T', confidence: 0.5, skip: true, details,
                reason: 'Không đủ tín hiệu đồng thuận',
                groupConsensus: { follow: 0, reverse: 0, learned: 0, pattern_db: 0, txt_table: 0 }
            };
        }

        const best = votes.T > votes.X ? 'T' : 'X';
        let confidence = Math.min(0.95, Math.max(0.55, Math.max(votes.T, votes.X) / total));

        // 🆕 Group consensus: nếu 2 nhóm mạnh (txt_table + pattern_db) cùng hướng → boost conf
        const txtDir = groupVotes.txt_table.T > groupVotes.txt_table.X ? 'T' : (groupVotes.txt_table.X > groupVotes.txt_table.T ? 'X' : null);
        const dbDir = groupVotes.pattern_db.T > groupVotes.pattern_db.X ? 'T' : (groupVotes.pattern_db.X > groupVotes.pattern_db.T ? 'X' : null);
        const learnedDir = groupVotes.learned.T > groupVotes.learned.X ? 'T' : (groupVotes.learned.X > groupVotes.learned.T ? 'X' : null);

        let consensusCount = 0;
        if (txtDir === best) consensusCount++;
        if (dbDir === best) consensusCount++;
        if (learnedDir === best) consensusCount++;
        if (consensusCount >= 2) confidence = Math.min(0.95, confidence + 0.07);
        if (consensusCount === 3) confidence = Math.min(0.95, confidence + 0.05);

        // 🆕 Selective prediction: SKIP khi conf < 0.68
        const skip = confidence < 0.68;

        // 🆕 Anti-stuck v2 (mạnh hơn): 5 dự đoán gần nhất cùng hướng và ≥4 sai → đảo
        const last5 = this.recentPreds.slice(-5);
        if (last5.length === 5 && last5.every(p => p === best)) {
            const actuals = this.recentActuals.slice(-5);
            const wrong = last5.filter((p, i) => p !== actuals[i]).length;
            if (wrong >= 4) {
                return {
                    prediction: best === 'T' ? 'X' : 'T',
                    confidence: 0.6,
                    skip: false,
                    details,
                    reason: `Anti-stuck: đảo do ${wrong}/5 gần nhất sai`,
                    groupConsensus: { follow: consensusCount, reverse: 0, learned: 0, pattern_db: 0, txt_table: 0 }
                };
            }
        }

        // 🆕 Calibrate theo bucket lịch sử
        let calibFactor = 1.0;
        const bucket = confidence < 0.65 ? '55-65' : confidence < 0.75 ? '65-75' : confidence < 0.85 ? '75-85' : '85-95';
        const b = this.calibBuckets[bucket];
        if (b.total >= 20) {
            const actualAcc = b.correct / b.total;
            const midBucket = bucket === '55-65' ? 0.60 : bucket === '65-75' ? 0.70 : bucket === '75-85' ? 0.80 : 0.90;
            calibFactor = actualAcc / midBucket;
            confidence = Math.min(0.95, Math.max(0.55, confidence * calibFactor));
        }

        return {
            prediction: best,
            confidence,
            skip,
            details,
            reason: skip ? 'Độ tin cậy thấp — chỉ tham khảo' : (consensusCount >= 2 ? `Đồng thuận ${consensusCount}/3 nhóm chính` : 'Đa số phiếu'),
            groupConsensus: {
                follow: +(groupVotes.follow.T + groupVotes.follow.X).toFixed(3),
                reverse: +(groupVotes.reverse.T + groupVotes.reverse.X).toFixed(3),
                learned: +(groupVotes.learned.T + groupVotes.learned.X).toFixed(3),
                pattern_db: +(groupVotes.pattern_db.T + groupVotes.pattern_db.X).toFixed(3),
                txt_table: +(groupVotes.txt_table.T + groupVotes.txt_table.X).toFixed(3)
            }
        };
    }
    recordActual(tx) {
        this.recentActuals.push(tx);
        if (this.recentActuals.length > 20) this.recentActuals.shift();
    }
    recordPrediction(tx, confidence) {
        this.recentPreds.push(tx);
        if (this.recentPreds.length > 20) this.recentPreds.shift();
        // 🆕 Cập nhật calibration bucket (khi có actual của phiên trước)
        if (this.recentActuals.length > 0) {
            const lastActual = this.recentActuals[this.recentActuals.length - 1];
            const bucket = confidence < 0.65 ? '55-65' : confidence < 0.75 ? '65-75' : confidence < 0.85 ? '75-85' : '85-95';
            this.calibBuckets[bucket].total++;
            if (tx === lastActual) this.calibBuckets[bucket].correct++;
        }
    }
}

// =====================================================================
// PATTERN MANAGER
// =====================================================================
class PatternManager {
    constructor() {
        this.history = [];
        this.ensemble = new Ensemble();
        this.currentPrediction = null;
        this.sinceRebalance = 0;
        this.predictionLog = {};
        this.streakCorrect = 0;
        this.streakWrong = 0;
    }
    init(history) {
        this.history = [...history];
        LEARNED_PATTERNS = learnPatternsFromHistory(this.history);
        STATS_FROM_DATA = computeStatsFromData(this.history);
        this.currentPrediction = this.ensemble.predict(this.history);
        console.log(`🧠 Học ${Object.keys(LEARNED_PATTERNS).length} mẫu từ ${this.history.length} phiên API`);
        console.log(`📊 PATTERN_TABLE: ${Object.keys(PATTERN_TABLE).length} entries | PATTERN_DB: ${PATTERN_DB.length} mẫu GitHub`);
        console.log(`🔮 Dự đoán ${this.history.at(-1).session + 1}: ${this.currentPrediction.prediction} (conf ${(this.currentPrediction.confidence * 100).toFixed(1)}%)`);
    }
    push(record) {
        if (this.currentPrediction) {
            const prev = this.currentPrediction;
            this.predictionLog[record.session] = {
                pred: prev.prediction,
                conf: prev.confidence,
                skip: prev.skip,
                reason: prev.reason
            };
            this.ensemble.recordPrediction(prev.prediction, prev.confidence);

            // Track streak correct/wrong (chỉ tính khi không skip)
            if (!prev.skip) {
                if (prev.prediction === record.tx) {
                    this.streakCorrect++;
                    this.streakWrong = 0;
                } else {
                    this.streakWrong++;
                    this.streakCorrect = 0;
                }
            }
        }
        this.ensemble.recordActual(record.tx);
        const prefix = this.history.slice();
        if (prefix.length >= 10) this.ensemble.updateWeights(prefix, record.tx);
        updateLearnedPatterns(this.history, record.tx);
        this.history.push(record);
        if (this.history.length > 500) this.history = this.history.slice(-450);

        this.sinceRebalance++;
        if (this.sinceRebalance >= 20) {
            this.ensemble.rebalance();
            this.sinceRebalance = 0;
            STATS_FROM_DATA = computeStatsFromData(this.history);
        }
        this.currentPrediction = this.ensemble.predict(this.history);
    }
}

const manager = new PatternManager();

// =====================================================================
// FETCH API
// =====================================================================
async function fetchAndProcessHistory() {
    try {
        const res = await fetch(API_URL);
        const data = await res.json();
        const newHistory = parseLines(data);
        if (newHistory.length === 0) return;

        const last = newHistory.at(-1);
        if (!currentSessionId) {
            manager.init(newHistory);
            txHistory = newHistory;
            currentSessionId = last.session;
            console.log(`✅ Đã tải ${newHistory.length} phiên API`);
        } else if (last.session > currentSessionId) {
            const newRecords = newHistory.filter(r => r.session > currentSessionId);
            for (const r of newRecords) {
                manager.push(r);
                txHistory.push(r);
            }
            if (txHistory.length > 350) txHistory = txHistory.slice(-300);
            currentSessionId = last.session;
            if (newRecords.length > 0) {
                console.log(`🆕 +${newRecords.length} phiên → ${currentSessionId} | pred ${manager.currentPrediction?.prediction} conf ${(manager.currentPrediction?.confidence * 100).toFixed(1)}%`);
            }
        }
    } catch (e) {
        console.error("❌ Lỗi fetch:", e.message);
    }
}

// =====================================================================
// FASTIFY SERVER
// =====================================================================
const app = fastify({ logger: true });
await app.register(cors, { origin: "*" });

app.get("/api/sunwin/tx", async () => {
    const last = txHistory.at(-1);
    const p = manager.currentPrediction;
    if (!last || !p) return {
        id: "@cskhgiabao", phien_truoc: null, xuc_xac: null,
        ket_qua: "đang chờ...", phien_nay: null, du_doan: "chưa có", do_tin_cay: "0%"
    };
    return {
        id: "@cskhgiabao",
        phien_truoc: last.session,
        xuc_xac: last.dice,
        ket_qua: last.result.toLowerCase(),
        phien_nay: last.session + 1,
        du_doan: p.prediction === 'T' ? 'tài' : 'xỉu',
        do_tin_cay: `${(p.confidence * 100).toFixed(0)}%`,
        nen_danh: p.skip ? 'THAM_KHAO' : 'DANH',
        ly_do: p.reason || '—',
        pattern_db_size: PATTERN_DB.length,
        pattern_table_size: Object.keys(PATTERN_TABLE).length,
        learned_patterns: Object.keys(LEARNED_PATTERNS).length,
        streak_correct: manager.streakCorrect,
        streak_wrong: manager.streakWrong
    };
});

app.get("/api/sunwin/history", async () => {
    if (!txHistory.length) return { message: "chưa có dữ liệu" };
    return [...txHistory].sort((a, b) => b.session - a.session).map(item => {
        const log = manager.predictionLog[item.session];
        let nhan_xet = "—";
        if (log) {
            const dung = log.pred === item.tx;
            const skipTag = log.skip ? ' [tham khảo]' : '';
            nhan_xet = dung
                ? `✅ Đúng (${log.pred === 'T' ? 'tài' : 'xỉu'} ${(log.conf * 100).toFixed(0)}%)${skipTag}`
                : `❌ Sai (${log.pred === 'T' ? 'tài' : 'xỉu'} ${(log.conf * 100).toFixed(0)}%)${skipTag}`;
        }
        return {
            session: item.session, dice: item.dice, total: item.total,
            result: item.result.toLowerCase(), tx_label: item.tx.toLowerCase(), nhan_xet
        };
    });
});

app.get("/api/balance/stats", async () => {
    const algoPerf = {};
    for (const a of ALL_ALGS) {
        const s = manager.ensemble.perAlgoStats[a.id];
        algoPerf[a.id] = {
            group: a.group,
            weight: +manager.ensemble.weights[a.id].toFixed(4),
            correct: s.correct, total: s.total,
            winrate: s.total > 0 ? +(s.correct / s.total * 100).toFixed(1) : null
        };
    }
    // 🆕 Calibration report
    const calibReport = {};
    for (const bucket in manager.ensemble.calibBuckets) {
        const b = manager.ensemble.calibBuckets[bucket];
        calibReport[bucket] = {
            total: b.total,
            correct: b.correct,
            actual_acc: b.total > 0 ? +(b.correct / b.total * 100).toFixed(1) : null
        };
    }
    return {
        pattern_db: { total: PATTERN_DB.length, source: PATTERN_FILE_URL },
        pattern_table_offline: { total: Object.keys(PATTERN_TABLE).length, source: "10 file list*.txt" },
        learned_patterns: { total: Object.keys(LEARNED_PATTERNS).length, sample_size: manager.history.length },
        stats: STATS_FROM_DATA,
        algorithms: algoPerf,
        calibration: calibReport,
        streak_correct: manager.streakCorrect,
        streak_wrong: manager.streakWrong
    };
});

app.get("/xem", async (req, reply) => {
    if (!txHistory.length) return reply.type("text/html").send("<h1>Chưa có dữ liệu</h1>");
    const rev = [...txHistory].sort((a, b) => b.session - a.session).slice(0, 100);
    let total = 0, correct = 0;
    for (const it of rev) {
        const log = manager.predictionLog[it.session];
        if (log && !log.skip) { total++; if (log.pred === it.tx) correct++; }
    }
    const acc = total > 0 ? (correct / total * 100).toFixed(1) : "0.0";
    const rows = rev.map(it => {
        const log = manager.predictionLog[it.session];
        let nx = "—", color = "#888";
        if (log) {
            const d = log.pred === it.tx;
            const skipTag = log.skip ? ' <span style="color:#ffaa00">[TK]</span>' : '';
            nx = d
                ? `✅ Đúng (${(log.conf * 100).toFixed(0)}%)${skipTag}`
                : `❌ Sai (${(log.conf * 100).toFixed(0)}%)${skipTag}`;
            color = d ? "#00ff88" : "#ff4444";
        }
        const c = it.tx === 'T' ? '#ff6b6b' : '#4ecdc4';
        return `<tr><td>${it.session}</td><td style="color:${c};font-weight:bold">${it.tx === 'T' ? 'TÀI' : 'XỈU'}</td><td>${it.total}</td><td style="color:${color};font-weight:bold">${nx}</td></tr>`;
    }).join("");
    const p = manager.currentPrediction;
    const next = txHistory.at(-1).session + 1;
    const confColor = p && p.confidence >= 0.72 ? '#00ff88' : (p && p.confidence >= 0.68 ? '#ffd700' : '#ff8888');
    const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="10"><title>AI Tài Xỉu v12</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{background:#0a0e1a;color:#e0e0e0;font-family:-apple-system,sans-serif;padding:12px}h1{font-size:18px;margin-bottom:8px;color:#ffd700}.box{background:linear-gradient(135deg,#1e3a5f,#2d1b4e);padding:14px;border-radius:8px;margin-bottom:12px;border:1px solid #ffd700}.pred{font-size:24px;font-weight:bold;color:${confColor};text-transform:uppercase}.stats{background:#1a1f2e;padding:12px;border-radius:8px;margin-bottom:12px;display:flex;gap:16px;flex-wrap:wrap;font-size:13px}.stats b{color:#ffd700}table{width:100%;border-collapse:collapse;background:#1a1f2e;border-radius:8px;overflow:hidden;font-size:13px}th{background:#252b3d;padding:10px 8px;text-align:left;color:#ffd700}td{padding:10px 8px;border-top:1px solid #252b3d}</style></head>
<body><h1>🎲 AI Tài Xỉu Sunwin v12</h1>
<div class="box"><h2>🔮 Dự đoán phiên ${next}</h2><div class="pred">${p ? (p.prediction === 'T' ? 'TÀI' : 'XỈU') : 'chờ'}</div>
<div style="color:#aaa;margin-top:4px">Tin cậy: <b style="color:${confColor}">${p ? (p.confidence * 100).toFixed(0) : 0}%</b> | ${p?.skip ? '⚠️ CHỈ THAM KHẢO' : '✅ TÍN HIỆU ĐỦ MẠNH'} | ${p?.reason || ''}</div>
<div style="color:#888;margin-top:4px;font-size:11px">TXTable: ${Object.keys(PATTERN_TABLE).length} | DB: ${PATTERN_DB.length} | Học online: ${Object.keys(LEARNED_PATTERNS).length}</div>
<div style="color:#888;font-size:11px;margin-top:2px">🔥 Streak đúng: <b style="color:#00ff88">${manager.streakCorrect}</b> | ❄️ Streak sai: <b style="color:#ff4444">${manager.streakWrong}</b></div></div>
<div class="stats"><span>Tổng: <b>${total}</b></span><span>Đúng: <b style="color:#00ff88">${correct}</b></span><span>Sai: <b style="color:#ff4444">${total - correct}</b></span><span>Tỷ lệ: <b>${acc}%</b></span></div>
<table><thead><tr><th>Phiên</th><th>KQ</th><th>Tổng</th><th>Nhận xét</th></tr></thead><tbody>${rows}</tbody></table>
<div style="text-align:center;color:#666;font-size:11px;margin-top:12px">Cập nhật mỗi 10s • ${new Date().toLocaleString('vi-VN')}</div>
</body></html>`;
    reply.type("text/html").send(html);
});
app.get("/", async () => ({
    status: "ok", msg: "AI Tài Xỉu Sunwin v12 - Hybrid Learning",
    version: "12.0",
    algorithms: ALL_ALGS.length,
    pattern_table_offline: Object.keys(PATTERN_TABLE).length,
    pattern_db_github: PATTERN_DB.length,
    learned_online: Object.keys(LEARNED_PATTERNS).length,
    endpoints: ["/api/sunwin/tx", "/api/sunwin/history", "/api/balance/stats", "/xem"]
}));

// =====================================================================
// START
// =====================================================================
const start = async () => {
    // 1. Tải 10k mẫu cầu GitHub (GIỮ NGUYÊN)
    PATTERN_DB = await fetchPatternFile(PATTERN_FILE_URL);
    if (PATTERN_DB.length) console.log(`✅ Pattern DB: ${PATTERN_DB.length} mẫu GitHub`);
    else console.log(`⚠️ Pattern DB rỗng, chỉ dùng TXTable + online learning`);

    console.log(`📊 PATTERN_TABLE (offline .txt): ${Object.keys(PATTERN_TABLE).length} entries`);
    console.log(`📊 CONTEXT_RULES: ${CONTEXT_RULES.length} rules | BREAK_PROBS: ${Object.keys(BREAK_PROBS).length} levels | MARKOV orders: ${Object.keys(MARKOV_TABLE).length}`);

    // 2. Start server
    try {
        await app.listen({ port: PORT, host: "0.0.0.0" });
    } catch (err) {
        const logFile = path.join(__dirname, "server-error.log");
        fs.writeFileSync(logFile, `[${new Date().toISOString()}] ${err.message}\n${err.stack}\n`, { flag: "a+" });
        console.error(err);
        process.exit(1);
    }

    // 3. Fetch API mỗi 5s
    fetchAndProcessHistory();
    clearInterval(fetchInterval);
    fetchInterval = setInterval(fetchAndProcessHistory, 5000);

    console.log(`\n🚀 AI Tài Xỉu Sunwin v12 - Hybrid Learning`);
    console.log(`   ➜ http://localhost:${PORT}/`);
    console.log(`   ➜ 10k mẫu cầu GitHub: ${PATTERN_DB.length}`);
    console.log(`   ➜ PATTERN_TABLE offline: ${Object.keys(PATTERN_TABLE).length}`);
    console.log(`   ➜ ${ALL_ALGS.length} thuật toán\n`);
};

start();