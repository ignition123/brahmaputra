// const PORT = 20000;
// const MULTICAST_ADDR = "233.255.255.255";

// const dgram = require("dgram");
// const process = require("process");

// const socket = dgram.createSocket({ type: "udp4", reuseAddr: true });

// socket.bind(PORT);

// socket.on("listening", function() {
//   socket.addMembership(MULTICAST_ADDR);
//   setInterval(sendMessage, 2500);
//   const address = socket.address();
//   console.log(
//     `UDP socket listening on ${address.address}:${address.port} pid: ${
//       process.pid
//     }`
//   );
// });

// function sendMessage() {
//   const message = Buffer.from(`Message from process ${process.pid}`);
//   socket.send(message, 0, message.length, PORT, MULTICAST_ADDR, function() {
//     console.info(`Sending message "${message}"`);
//   });
// }

// socket.on("message", function(message, rinfo) {
//   console.info(`Message from: ${rinfo.address}:${rinfo.port} - ${message}`);
// });

var PORT = 6024;
var BROADCAST_ADDR = "192.168.101.9";
var dgram = require('dgram');
var server = dgram.createSocket("udp4");

server.bind(function() {
    server.setBroadcast(true);
});

server.on('message', function (message, rinfo) {
    broadcastNew(message);
});


function broadcastNew(message) {
    message = Buffer.from(message);
    server.send(message, 0, message.length, PORT, BROADCAST_ADDR, function() {
        console.log("Sent '" + message + "'");
    });
}