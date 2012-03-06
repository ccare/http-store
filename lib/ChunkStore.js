var MemoryStream = require('memstream').MemoryStream;
var timers = require('timers')
var Streamable = require('./core').Streamable

function Item(headers, data) {
    this.headers = headers
    this.data = data
}

function ChunkStore(innerStore) {
    this.inner = innerStore
}

var FIFTY_MEG = 50 * 1024 * 1024
var ONE_MEG = 1024 * 1024

var CHUNK_CONTENT_TYPE = "x-chunk"
var MANIFEST_CONTENT_TYPE = "application/vnd.chunkstore.manifest"

ChunkStore.prototype.put = function(key, headers, callback) {
    var self = this
    return self.doPut(key, headers, function(err, chunkRefs) {
        var json = JSON.stringify(chunkRefs)
        var stream = self.inner.put(key,
            {
                'content-length' : json.length,
                'content-type' : MANIFEST_CONTENT_TYPE
            },
            function(err, res) {
                console.log("manifest E: %s", err)
                console.log("manifest R: %s", res)
                callback(err, res)
            })
        stream.write(json)
        stream.end()
    })
}

ChunkStore.prototype.doPut = function(key, headers, callback) {
    var self = this
    var chunks = []
    var chunkRefs = []
    var currentChunkSize = 0
    var chunkCounter = 0;
    var semaphore = {
        chunkCount : 0,
        allSubmitted: false
    }
    var outer = new MemoryStream(function(buffer) {
        var currentBufferLength = buffer.length 
        if (currentChunkSize+currentBufferLength >= ONE_MEG) {
            console.log("Rolling over %d", currentChunkSize)
            var chunkKey = "chunks/" + key + "/" + chunkCounter
            chunkRefs.push(chunkKey)
            self.submitChunks(chunkKey, chunks, function(err, res) {
                semaphore.chunkCount--
                if (semaphore.allSubmitted && semaphore.chunkCount == 0) {
                    callback(err, chunkRefs)
                }
            })
            chunks = []
            currentChunkSize = 0
            chunkCounter++
            semaphore.chunkCount++
        }
        chunks.push(buffer)
        currentChunkSize += buffer.length
    })
    semaphore.allSubmitted = true
    outer.on('end', function() {
        var chunkKey = "chunks/" + key + "/" + chunkCounter
        chunkRefs.push(chunkKey)
        self.submitChunks(chunkKey, chunks, function(err, res) {
            semaphore.chunkCount--
            if (semaphore.allSubmitted && semaphore.chunkCount == 0) {
                callback(err, chunkRefs)
            }
        })
    })
    return outer
}


ChunkStore.prototype.submitChunks = function(id, chunks, callback) {
    console.log("Submitting chunk of %d buffers", chunks.length)
    if (chunks.length > 0) {
        var clength = 0
        for (var i in chunks) {
            clength += chunks[i].length
        }
        var stream = this.inner.put(id,
            {'content-length' : clength},
            function(err, res) {
                console.log("E: %s", err)
                console.log("R: %s", res)
                callback(err, res)
            })
        for (var i in chunks) {
            stream.write(chunks[i])
        }
        stream.end()
    }
}

ChunkStore.prototype.get = function(key, callback) {
    var self = this
    self.inner.get(key, function(err, item) {
        var contentType = item.headers['content-type']
        if (contentType == MANIFEST_CONTENT_TYPE) {
            var outStream = new MemoryStream()
            var manStream = new MemoryStream(function(buffer) {
                var manifestJSON = JSON.parse(buffer.toString())
                console.log('dereferencing')
                self.dereference(0, manifestJSON, outStream)
            })
            item.stream.pipe(manStream)
            item.stream = outStream
            callback(err, item)
        } else {
            callback(err, item)
        }        
    })
}

ChunkStore.prototype.dereference = function(idx, manifest, outStream) {
    var self = this
    if (idx < manifest.length) {
        console.log('dereferencing')
        self.inner.get(manifest[idx], function(e, i) {
            i.stream.pipe(outStream)
            self.dereference(idx+1, manifest, outStream)
        })
    }
}

module.exports.ChunkStore = ChunkStore