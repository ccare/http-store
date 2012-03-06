var express = require('express')
var RamStore = require('./lib/RamStore').RamStore
var S3Store = require('./lib/S3Store').S3Store
var ChunkStore = require('./lib/ChunkStore').ChunkStore

var store = new ChunkStore(new S3Store())

var app = express.createServer()
// GET
app.get('/:key', function(req, res) {
    var key = req.params.key;
    store.get(key, function(err, item) {
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
})
// PUT
app.put('/:key', function(req, res) {
    var key = req.params.key;
    var stream = store.put(key, req.headers, function() {
        res.send('stored', 201)
    })
    req.pipe(stream)
})
// DELETE
app.delete('/:key', function(req, res) {
    var key = req.params.key;
    var stream = store.delete(key, function() {
        res.send('stored', 201)
    })
})
app.listen(3000)
