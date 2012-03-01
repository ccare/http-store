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

function ItemStore() {
    this.items = {
        key1 : new Item({ 'content-type': 'text/plain' }, "my data"),
        key2 : new Item({ 'content-type': 'text/turtle' }, "my other data")   
    }
}
ItemStore.prototype.put = function(key, headers) {
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
ItemStore.prototype.get = function(key) {
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

var store = new ItemStore()

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
    var stream = store.put(key, req.headers)
    req.pipe(stream)
    res.send('stored', 201)
})
app.listen(3000)
