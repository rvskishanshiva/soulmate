const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const sqlite3 = require("sqlite3").verbose();
const fs = require("fs");
const path = require("path");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    maxHttpBufferSize: 5e7 // 50 MB limits for file sharing
});

app.use(express.static("public"));

const config = JSON.parse(fs.readFileSync("config.json"));
const CHAT_PASSCODE = config.chatPasscode;
const ALLOWED_USERS = ["Sunshine", "Angel"];

let onlineUsers = 0;

// Render uses a dedicated disk directory; fallback to local project path if absent
const dbPath = process.env.RENDER_DISK_PATH 
    ? path.join(process.env.RENDER_DISK_PATH, 'chat.db') 
    : path.join(__dirname, 'chat.db');

if (!fs.existsSync(DB_DIR)) {
    fs.mkdirSync(DB_DIR, { recursive: true });
}

const db = new sqlite3.Database(dbPath, (err) => {
    if (err) console.error('Database opening error:', err.message);
    else console.log('Connected to SQLite database at:', dbPath);
});

db.serialize(() => {
    db.run(`
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_name TEXT,
            text TEXT,
            created_at DATETIME,
            parent_id INTEGER DEFAULT NULL,
            reactions TEXT DEFAULT '{}'
        )
    `);
});

io.on("connection", (socket) => {
    socket.authenticated = false;
    socket.username = "";

    onlineUsers++;
    io.emit("online-users", onlineUsers);

    socket.on("check-passcode", (data) => {
        if (!data) return socket.emit("access-denied");
        const formattedName = data.name.trim();

        if (data.code === CHAT_PASSCODE && ALLOWED_USERS.includes(formattedName)) {
            socket.authenticated = true;
            socket.username = formattedName;
            socket.emit("access-granted", { username: socket.username });

            db.all("SELECT * FROM messages ORDER BY id ASC", [], (err, rows) => {
                socket.emit("load-messages", rows);
            });
        } else {
            socket.emit("access-denied");
        }
    });

    socket.on("typing", () => {
        if (socket.authenticated) {
            socket.broadcast.emit("typing", socket.username);
        }
    });

    socket.on("chat-message", (data) => {
        if (!socket.authenticated || !data) return;
        
        const messageText = typeof data === "string" ? data : data.text;
        const parentId = data.parentId || null;
        const now = new Date().toLocaleString();

        db.run(
            `INSERT INTO messages (sender_name, text, created_at, parent_id, reactions) VALUES (?, ?, ?, ?, '{}')`,
            [socket.username, messageText, now, parentId],
            function (err) {
                if (err) return;
                io.emit("chat-message", {
                    id: this.lastID,
                    sender_name: socket.username,
                    text: messageText,
                    created_at: now,
                    parent_id: parentId,
                    reactions: {}
                });
            }
        );
    });

    socket.on("toggle-reaction", (data) => {
        if (!socket.authenticated || !data) return;
        const { messageId, emoji } = data;

        db.get("SELECT reactions FROM messages WHERE id = ?", [messageId], (err, row) => {
            if (err || !row) return;

            let reactions = {};
            try { reactions = JSON.parse(row.reactions || "{}"); } catch(e) { reactions = {}; }

            if (!reactions[emoji]) reactions[emoji] = [];

            const userIndex = reactions[emoji].indexOf(socket.username);
            if (userIndex > -1) {
                reactions[emoji].splice(userIndex, 1);
                if (reactions[emoji].length === 0) delete reactions[emoji];
            } else {
                Object.keys(reactions).forEach(key => {
                    const idx = reactions[key].indexOf(socket.username);
                    if (idx > -1) {
                        reactions[key].splice(idx, 1);
                        if (reactions[key].length === 0) delete reactions[key];
                    }
                });
                reactions[emoji].push(socket.username);
            }

            const updatedReactionsStr = JSON.stringify(reactions);
            db.run("UPDATE messages SET reactions = ? WHERE id = ?", [updatedReactionsStr, messageId], () => {
                io.emit("reaction-updated", { messageId, reactions });
            });
        });
    });

    socket.on("edit-message", (data) => {
        if (!socket.authenticated) return;
        db.get("SELECT sender_name FROM messages WHERE id = ?", [data.id], (err, row) => {
            if (row && row.sender_name === socket.username) {
                db.run("UPDATE messages SET text = ? WHERE id = ?", [data.newText, data.id], () => {
                    io.emit("message-edited", { id: data.id, text: data.newText });
                });
            }
        });
    });

    socket.on("delete-messages", (ids) => {
        if (!socket.authenticated || !Array.isArray(ids) || ids.length === 0) return;
        const cleanIds = ids.map(id => Number(id)).filter(id => !isNaN(id));
        if (cleanIds.length === 0) return;

        const placeholders = cleanIds.map(() => "?").join(",");
        db.all(`SELECT id FROM messages WHERE id IN (${placeholders}) AND sender_name = ?`, [...cleanIds, socket.username], (err, rows) => {
            if (err || !rows) return;
            
            const verifiedIds = rows.map(row => row.id);
            if (verifiedIds.length === 0) return;

            const verifiedPlaceholders = verifiedIds.map(() => "?").join(",");
            db.run(`DELETE FROM messages WHERE id IN (${verifiedPlaceholders})`, verifiedIds, (deleteErr) => {
                if (!deleteErr) {
                    io.emit("messages-deleted", verifiedIds);
                }
            });
        });
    });

    socket.on("disconnect", () => {
        onlineUsers--;
        io.emit("online-users", onlineUsers);
    });
});

// Capture Cloud Production environment Ports automatically
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
