var knox = require('knox')
var Streamable = require('./core').Streamable

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

var s3client = knox.createClient({
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
    var newHeaders = {'content-length' : contentLength, 'Content-Type': headers['content-type'] }
    //for (h in headers) {
    //    if (h == 'content-type' || h[0] == 'X' || h[0] == 'x') {
    //        var val = headers[h]
    //        console.log('setting header %s as %s', h, val)
    //        newHeaders[h] = val
    //    }
    //}
    console.log("HEADERS are %j", newHeaders)
    var stream = self.client.put('data/' + key, newHeaders);
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
S3Store.prototype.get = function(key, callback) {
    console.log("Getting %s", key)
    var self = this
    self.client.get('data/' + key).on('response', function(res){
        console.log(res.statusCode);
        console.log(res.headers);
        if (200 == res.statusCode) {
            res.setEncoding('utf8');
            var item = new Streamable({}, res)
            for (h in res.headers) {
                if (h == 'content-type' || h == 'Content-Type' ||h[0] == 'X' || h[0] == 'x') {
                    var val = res.headers[h]
                    console.log('reading header %s as %s', h, val)
                    item.headers[h] = val
                }
            }
            callback(null, item)
        } else if (404 == res.statusCode) {
            callback(null, null)
        } else {
            callback({
                error: 'bad status code from S3',
                statusCode: res.statusCode
            }, null)
        }
    }).end();
}

module.exports.S3Store = S3Store