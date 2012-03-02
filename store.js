var express = require('express')
var MemoryStream = require('memstream').MemoryStream;
var timers = require('timers')

function Item(headers, data) {
    this.headers = headers
    this.data = data
}

function Streamable(headers, stream) {
    this.headers = headers
    this.stream = stream
}

function RamStore() {
    this.items = {
        key1 : new Item({ 'content-type': 'text/plain' }, "my data"),
        key2 : new Item({ 'content-type': 'text/turtle' }, "my other data")   
    }
}
RamStore.prototype.put = function(key, headers) {
    console.log("Putting %s with headers %j", key, headers)
    var self = this
    var newItem = new Item({}, "")
    for (h in headers) {
        if (h == 'content-type' || h[0] == 'X' || h[0] == 'x') {
            var val = headers[h]
            console.log('saving header %s as %s', h, val)
            newItem.headers[h] = val
        }
    }
    var stream = new MemoryStream(function(buffer) {
        newItem.data += String(buffer)
    })
    stream.on('end', function() {
        self.items[key] = newItem
    })
    return stream
}
RamStore.prototype.get = function(key) {
    console.log("Getting %s", key)
    var self = this
    var item = self.items[key]
    if (null == item || undefined == item) {
        return null
    } else {
        var stream = new MemoryStream()
        timers.setTimeout(function() {
            stream.write(item.data)
            stream.end()
        }, 100)
        return new Streamable(item.headers, stream)
    }
}

var S3_KEY = process.env.AWS_ACCESS_KEY_ID;
var S3_SECRET = process.env.AWS_SECRET_ACCESS_KEY;
var S3_BUCKET = 'ccare';


function S3Store() {
    this.client = require('knox').createClient({
        key: S3_KEY,
        secret: S3_SECRET,
        bucket: S3_BUCKET
    });
}

var s3client = require('knox').createClient({
    key: S3_KEY,
    secret: S3_SECRET,
    bucket: S3_BUCKET
});

S3Store.prototype.put = function(key, headers, callback) {
    console.log("Putting %s with headers %j", key, headers)
    var self = this
    var contentLength = headers['content-length']
    if (contentLength == null) {
        throw "content length needed"
    }
    var stream = self.client.put('data/' + key,
        {'content-length' : contentLength});
    stream.on('response', function(res){
      console.log("resp %s", res.statusCode);
      callback(null, res)
    });
    stream.on('error', function(err){
      console.log("err %s", err);
      callback(err, null)
    });
    return stream
}
S3Store.prototype.get = function(key) {
    console.log("Getting %s", key)
    var self = this
    var item = self.items[key]
    if (null == item || undefined == item) {
        return null
    } else {
        var stream = new MemoryStream()
        timers.setTimeout(function() {
            stream.write(item.data)
            stream.end()
        }, 100)
        return new Streamable(item.headers, stream)
    }
}
















var store = new S3Store()

var app = express.createServer()
app.use(express.bodyParser())
app.get('/:key', function(req, res) {
    var key = req.params.key;
    var item = store.get(key)
    if (null == item) {
        res.header('content-type', 'text/plain')
        res.send("Not found\n", 404)
    } else {
        for (h in item.headers) {
            var val = item.headers[h]
            console.log('setting header %s to %s', h, val)
            res.header(h, val)
        }
        var stream = item.stream
        stream.pipe(res)
    }
})
app.put('/:key', function(req, res) {
    var key = req.params.key;
    var stream = store.put(key, req.headers, function(storeErr, storeRes) {
        res.send('stored', 201)
    })
    req.pipe(stream)
})
app.listen(3000)
