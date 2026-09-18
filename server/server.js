const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { randomInt, randomUUID } = require("node:crypto");
const { WebSocketServer, WebSocket } = require("ws");

// 房间码 -> 房间成员集合。
const rooms = new Map();

// WebSocket 连接 -> 会话信息。
const sessions = new Map();
// 房间码 -> 当前双人配对的标识。
// 房间只有一名成员时，不保留配对标识。
const pairIds = new Map();

const page = fs.readFileSync(path.join(__dirname, "test.html"));

const server = http.createServer((req, res) => {
  if (req.url === "/") {
    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
    });
    res.end(page);
    return;
  }

  res.writeHead(404);
  res.end("Not Found");
});

const wss = new WebSocketServer({
  server,
  path: "/ws",
  maxPayload: 64 * 1024,
});

// 统一发送 JSON 消息。
function send(socket, message) {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}

function sendError(socket, message) {
  send(socket, {
    type: "error",
    message,
  });
}

// 生成未使用的六位数字房间码。
// 限制尝试次数，避免在异常情况下无限循环。
function generateRoomId() {
  for (let attempt = 0; attempt < 100; attempt++) {
    const roomId = String(randomInt(100000, 1000000));

    if (!rooms.has(roomId)) {
      return roomId;
    }
  }

  return null;
}

// 主动离开和连接关闭都复用此函数。
function leaveRoom(socket, notifySelf = true) {
  const session = sessions.get(socket);

  if (!session || !session.roomId) {
    if (notifySelf) {
      sendError(socket, "你尚未加入房间。");
    }
    return;
  }

  const roomId = session.roomId;
  const room = rooms.get(roomId);
  // 立即废弃上一轮配对。
  // 即使旧成员的信令稍后到达，也不能继续转发。
  pairIds.delete(roomId);

  // 保留 WebSocket 会话，但清除房间归属。
  session.roomId = null;

  if (room) {
    room.delete(socket);

    for (const peer of room) {
      send(peer, {
        type: "peer-left",
        roomId,
      });
    }

    // 剩余成员可以继续等待新设备加入。
    // 最后一名成员离开时才删除房间。
    if (room.size === 0) {
      rooms.delete(roomId);
    }
  }

  if (notifySelf) {
    send(socket, {
      type: "room-left",
      roomId,
    });
  }

  console.log(`成员离开房间：${roomId}`);
}

function handleMessage(socket, message) {
  const session = sessions.get(socket);

  if (!session) {
    return;
  }

  switch (message.type) {
    case "create-room": {
      if (session.roomId) {
        sendError(socket, "请先离开当前房间。");
        return;
      }

      const roomId = generateRoomId();

      if (!roomId) {
        sendError(socket, "暂时无法创建房间，请稍后重试。");
        return;
      }

      rooms.set(roomId, new Set([socket]));
      session.roomId = roomId;

      send(socket, {
        type: "room-created",
        roomId,
      });

      console.log(`房间已创建：${roomId}`);
      break;
    }

    case "join-room": {
      if (session.roomId) {
        sendError(socket, "请先离开当前房间。");
        return;
      }

      if (
        typeof message.roomId !== "string" ||
        !/^\d{6}$/.test(message.roomId)
      ) {
        sendError(socket, "房间码必须是六位数字。");
        return;
      }

      const roomId = message.roomId;
      const room = rooms.get(roomId);

      if (!room) {
        sendError(socket, "房间不存在，请检查房间码。");
        return;
      }

      if (room.size >= 2) {
        sendError(socket, "房间已满，最多允许两名成员。");
        return;
      }

      room.add(socket);
      session.roomId = roomId;

      // 每次有第二名成员加入，都建立一轮新的配对。
      const pairId = randomUUID();
      pairIds.set(roomId, pairId);

      // 通知新加入的成员。
      send(socket, {
        type: "room-joined",
        roomId,
        pairId,
      });

      // 通知已经在房间里的成员。
      for (const peer of room) {
        if (peer !== socket) {
          send(peer, {
            type: "peer-joined",
            roomId,
            pairId,
          });
        }
      }

      console.log(`成员加入房间：${roomId}，配对：${pairId}`);
      break;
    }

    case "relay": {
      if (!session.roomId) {
        sendError(socket, "请先创建或加入房间。");
        return;
      }

      if (
        typeof message.text !== "string" ||
        message.text.trim().length === 0 ||
        message.text.length > 1000
      ) {
        sendError(socket, "消息必须是 1～1000 个字符的非空文本。");
        return;
      }

      const room = rooms.get(session.roomId);

      if (!room) {
        sendError(socket, "当前房间不存在。");
        return;
      }

      // 转发目标由服务器记录决定，不接受客户端指定其他房间。
      const peer = [...room].find(
        (member) =>
          member !== socket &&
          member.readyState === WebSocket.OPEN
      );

      if (!peer) {
        sendError(socket, "对方尚未加入或已经断开。");
        return;
      }

      send(peer, {
        type: "relay",
        text: message.text,
      });

      break;
    }

    case "leave-room": {
      leaveRoom(socket);
      break;
    }

    case "signal": {
      if (!session.roomId) {
        sendError(socket, "请先加入房间。");
        return;
      }

      const currentPairId = pairIds.get(session.roomId);

      if (
        !currentPairId ||
        typeof message.pairId !== "string" ||
        message.pairId !== currentPairId
      ) {
        // 旧信令在离开、重新配对时可能正常出现，直接丢弃。
        console.log("已丢弃过期或缺少配对标识的信令");
        return;
      }

      const signal = message.signal;

      if (
        !signal ||
        typeof signal !== "object" ||
        !["offer", "answer", "candidate"].includes(signal.kind)
      ) {
        sendError(socket, "无效的 WebRTC 信令。");
        return;
      }

      if (
        ["offer", "answer"].includes(signal.kind) &&
        (
          !signal.description ||
          signal.description.type !== signal.kind ||
          typeof signal.description.sdp !== "string"
        )
      ) {
        sendError(socket, "无效的连接描述。");
        return;
      }

      if (
        signal.kind === "candidate" &&
        (
          !signal.candidate ||
          typeof signal.candidate.candidate !== "string"
        )
      ) {
        sendError(socket, "无效的 ICE Candidate。");
        return;
      }

      const room = rooms.get(session.roomId);

      const peer = room && [...room].find(
        (member) =>
          member !== socket &&
          member.readyState === WebSocket.OPEN
      );

      if (!peer) {
        sendError(socket, "对方不在线，无法转发信令。");
        return;
      }

      send(peer, {
        type: "signal",
        pairId: currentPairId,
        signal,
      });

      console.log(`已转发信令：${signal.kind}`);
      break;
    }

    default: {
      sendError(socket, "不支持的消息类型。");
    }
  }
}

wss.on("connection", (socket) => {
  sessions.set(socket, {
    roomId: null,
  });

  console.log("浏览器已连接");

  socket.on("message", (data, isBinary) => {
    if (isBinary) {
      sendError(socket, "当前只接受 JSON 文本消息。");
      return;
    }

    let message;

    try {
      message = JSON.parse(data.toString());
    } catch {
      sendError(socket, "消息不是合法的 JSON。");
      return;
    }

    if (
      message === null ||
      typeof message !== "object" ||
      Array.isArray(message) ||
      typeof message.type !== "string"
    ) {
      sendError(socket, "消息必须是包含 type 字段的对象。");
      return;
    }

    handleMessage(socket, message);
  });

  socket.on("close", () => {
    // 连接已经关闭，无须再给自己发送离开确认。
    leaveRoom(socket, false);
    sessions.delete(socket);

    console.log("浏览器已断开");
  });

  socket.on("error", (error) => {
    console.error("WebSocket 错误：", error.message);
  });
});

server.on("error", (error) => {
  console.error("服务器错误：", error.message);
});

server.listen(3000, "0.0.0.0", () => {
  console.log("服务器已启动");
  console.log("本机访问：http://127.0.0.1:3000");
  console.log("其他设备访问：http://本机局域网IP:3000");
});