var PORT = 6021;
var BROADCAST_ADDR = "192.168.101.9";
var dgram = require('dgram');
var client = dgram.createSocket({type: 'udp4', reuseAddr: true});

client.on('listening', function () {
    var address = client.address();
    console.log('UDP Client listening on ' + address.address + ":" + address.port);
    client.setBroadcast(true);
});

client.on('message', function (message, rinfo) {
    console.log('Message from: ' + rinfo.address + ':' + rinfo.port +' - ' + message);
});

var message = Buffer.from("Hello");

client.send(message, 0, message.length, 6021, function() {
	console.log("Sent '" + message + "'");
});

client.bind(PORT);