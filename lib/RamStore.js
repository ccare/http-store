var MemoryStream = require('memstream').MemoryStream;
var timers = require('timers')
var Streamable = require('./core').Streamable

function Item(headers, data) {
    this.headers = headers
    this.data = data
}

function RamStore() {
    this.items = {
        key1 : new Item({ 'content-type': 'text/plain' }, "my data"),
        key2 : new Item({ 'content-type': 'text/turtle' }, "my other data")   
    }
}

RamStore.prototype.put = function(key, headers, callback) {
    console.log("Putting %s with headers %j", key, headers)
    var self = this
    var newItem = new Item({}, "")
    for (h in headers) {
        if (h == 'content-type' || h[0] == 'X' || h[0] == 'x') {
            var val = headers[h]
            newItem.headers[h] = val
        }
    }
    var stream = new MemoryStream(function(buffer) {
        newItem.data += String(buffer)
    })
    stream.on('end', function() {
        self.items[key] = newItem
        callback()
    })
    return stream
}

RamStore.prototype.get = function(key, callback) {
    console.log("Getting %s", key)
    var self = this
    var item = self.items[key]
    if (null == item || undefined == item) {
        callback("not found", null)
    } else {
        var stream = new MemoryStream()
        timers.setTimeout(function() {
            stream.write(item.data)
            stream.end()
        }, 100)
        callback(null, new Streamable(item.headers, stream))
    }
}

module.exports.RamStore = RamStore