const net = require('net');

const client = net.createConnection({port: 5001}, () => {
    console.log('Client: connected');
    client.write('this is client')
});

