var Stream  = require('stream'),
    AWS     = require('aws-sdk'),
    mime    = require('mime');

module.exports = function setup(fsOptions) {

    var client;
    initialize();

    return {
        readfile:   readfile,
        mkfile:     mkfile,
        rmfile:     rmfile,
        readdir:    readdir,
        stat:       stat,
        mkdir:      mkdir,
        rmdir:      rmdir,
        rename:     rename,
        copy:       copy,
        symlink:    symlink
    };

    function initialize() {
        var options = {
            accessKeyId: fsOptions.key,
            secretAccessKey: fsOptions.secret
        };

        client =  new AWS.S3(options);
    }

    function getPaths(path) {
        var paths = path.split('/').filter(function (path) {
            return path !== '';
        });

        return {
            bucket: paths[0],
            path:   paths.slice(1).join('/')
        };
    }

    function readfile(path, options, callback) {
        var paths = getPaths(path);
        var params = { Bucket: paths.bucket, Key: paths.path };
        var meta = {};

        client.headObject(params, function (err, headers) {
            if (err) return callback(err);

            meta.etag = headers.ETag;
            meta.size = headers.ContentLength;
            meta.mime = headers.ContentType;

            if (options.etag) {
                params.IfNoneMatch = options.etag;
            }

            if (meta.etag === options.etag) {
                meta.notModified = true;
                return callback(null, meta);
            }

            meta.stream = client.getObject(params).createReadStream();
            callback(null, meta);
        });
    }

    function mkfile(path, options, callback) {
        callback(new Error('mkfile: Not Implemented'));
    }

    function rmfile(path, options, callback) {
        callback(new Error('rmfile: Not Implemented'));
    }

    function readdir(path, options, callback) {
        var paths = getPaths(path);
        var prefix;

        var meta = {};

        if (paths.bucket) {
            prefix = paths.path;

            if (prefix.length !== 0) {
                prefix = paths.path + '/';
            }

            client.listObjects({ Prefix: prefix, Delimiter: '/', Bucket: paths.bucket }, processCallback);
        } else {
            client.listBuckets(processCallback);
        }

        function processCallback (err, data) {
            if (err) return callback(err);

            var stream = new Stream();
            stream.readable = true;

            var paused;

            stream.pause = function () {
                if (paused === true) return;
                paused = true;
            };

            stream.resume = function () {
                if (paused === false) return;
                paused = false;
                getNext();
            };

            var children;
            if (data.Buckets) {
                children = data.Buckets;
            } else {
                children = data.CommonPrefixes.concat(data.Contents);
            }

            meta.stream = stream;
            callback(null, meta);

            var index = 0;
            stream.resume();

            function getNext () {
                if (index === children.length) return done();
                var child = children[index++];
                var left = children.length - index,
                    statEntry;

                if (data.Buckets) {
                    statEntry = { bucket: child };
                } else {
                    statEntry = { prefix: prefix, child: child, bucket: paths.bucket };
                }

                createStatEntry(statEntry, function (err, entry) {
                    if (err) {
                        stream.emit('error', err);
                    } else {
                        stream.emit("data", entry);
                    }
                    if (!paused) getNext();
                });
            }

            function done() {
                stream.emit("end");
            }

        }
    }

    function stat(path, options, callback) {
        var paths = getPaths(path);
        var prefix;

        var meta = {};

        if (paths.bucket) {
            if (paths.path) {
                client.headObject({ Bucket: paths.bucket, Key: paths.path }, function (err, data) {
                    if (err) {
                        if (err.statusCode === 404 && err.code === 'NotFound') {
                            // always a directory if not found
                            meta.mime = 'inode/directory';
                            meta.name = paths.path.substr(paths.path.lastIndexOf('/') + 1);
                            meta.path = '/' + paths.bucket + '/' + paths.path;
                            meta.size = 0;
                            meta.access = 4;
                            return callback(null, meta);
                        } else {
                            return callback(err);
                        }
                    }

                    meta.name = paths.path.substr(paths.path.lastIndexOf('/') + 1)
                    meta.mime = data.ContentType;
                    meta.size = data.ContentLength;
                    meta.etag = data.ETag;
                    meta.path = path;
                    meta.access = 4;
                    callback(null, meta);
                });
            } else {
                client.headBucket({ Bucket: paths.bucket }, function (err, data) {
                    if (err) return callback(err);
                    meta.mime = 'inode/directory';
                    meta.name = paths.bucket;
                    meta.path = '/' + paths.bucket;
                    meta.size = 0;
                    callback(null, meta);
                });
            }
        } else {
            meta.mime = 'inode/directory';
            meta.name = '/';
            meta.path = '/';
            meta.size = 0;
            process.nextTick(function () {
                callback(null, meta);
            });
        }
    }

    function mkdir(path, options, callback) {
        var paths = getPaths(path);
        if (!paths.bucket) {
            return callback(new Error('mkdir: creating root directory not allowed'));
        } else if (!paths.path) {
            return callback(new Error('mkdir: creating bucket not supported'));
        } else {
            client.putObject({ Bucket: paths.bucket, Key: paths.path + '/' }, function (err, data) {
                if (err) return callback(err);
                callback(null, {});
            });
        }
    }

    function rmdir(path, options, callback) {
        callback(new Error('rmdir: Not Implemented'));
    }

    function copy(path, options, callback) {
        callback(new Error('copy: Not Implemented'));
    }

    function rename(path, options, callback) {
        callback(new Error('rename: Not Implemented'));
    }

    function symlink(path, options, callback) {
        callback(new Error('symlink: Not Implemented'));
    }

    function createStatEntry(options, callback) {
        var entry = {};

        if (!options.child && options.bucket) {
            entry.id = '/' + options.bucket.Name;
            entry.name = options.bucket.Name;
            entry.access = 4 | (true ? 2 : 0); // file.editable == true
            entry.size = 0;
            entry.mtime = (new Date()).valueOf(); // (new Date(file.modifiedDate)).valueOf()
            entry.mime = 'inode/directory';
            callback(null, entry);
        } else if (options.child.Prefix) {
            entry.id = '/' + options.bucket + '/' + removeTrailingSlash(options.child.Prefix);
            entry.name = entry.id.substr(entry.id.lastIndexOf('/') + 1);
            entry.access = 4 | (true ? 2 : 0); // file.editable == true
            entry.size = 0;
            entry.mtime = (new Date()).valueOf(); // (new Date(file.modifiedDate)).valueOf()
            entry.mime = 'inode/directory';
            callback(null, entry);
        } else if (options.child.Key) {
            entry.id = '/' + options.bucket + '/' + options.child.Key;
            entry.name = entry.id.substr(entry.id.lastIndexOf('/') + 1);
            entry.access = 4 | (true ? 2 : 0); // file.editable == true
            entry.size = options.child.Size,
            entry.mtime = options.child.LastModified;
            entry.mime = mime.lookup(entry.name);
            callback(null, entry);
        } else {
            callback(new Error('createStatEntry: Not Supported'));
        }
    }

    function removeTrailingSlash (str) {
        if(str.substr(-1) == '/') {
            return str.substr(0, str.length - 1);
        }
        return str;
    }

};
