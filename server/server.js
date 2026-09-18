const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { WebSocketServer } = require("ws");

// 启动时读取测试页面。
const page = fs.readFileSync(path.join(__dirname, "test.html"));

// HTTP 服务负责把测试页面交给浏览器。
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

// WebSocket 与 HTTP 共用一个端口。
const wss = new WebSocketServer({
  server,
  path: "/ws",
  maxPayload: 64 * 1024,
});

// 每当一个浏览器连接进来，就执行这个函数。
// socket 代表与这个浏览器之间的连接。
wss.on("connection", (socket) => {
  console.log("一个浏览器已连接");

  socket.send("你好，浏览器！服务器连接成功。");

  socket.on("message", (data, isBinary) => {
    if (isBinary) {
      socket.send("当前测试只接受文本消息。");
      return;
    }

    const text = data.toString();

    console.log("收到浏览器消息：", text);
    socket.send(`服务器已收到：${text}`);
  });

  socket.on("close", () => {
    console.log("一个浏览器已断开");
  });

  socket.on("error", (error) => {
    console.error("连接错误：", error.message);
  });
});

server.on("error", (error) => {
  console.error("服务器错误：", error.message);
});

// 当前先在本机测试。
server.listen(3000, "127.0.0.1", () => {
  console.log("服务器已启动，请打开 http://127.0.0.1:3000");
});