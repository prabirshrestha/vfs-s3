var Stream  = require('stream'),
    knox    = require('knox'),
    mime    = require('mime');

module.exports = function setup(fsOptions) {

    var client = knox.createClient({
        key:    fsOptions.key,
        secret: fsOptions.secret,
        bucket: fsOptions.bucket
    });

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

    function readfile(path, options, callback) {
        callback(new Error('readfile: Not Implemented'));
    }

    function mkfile(path, options, callback) {
        callback(new Error('mkfile: Not Implemented'));
    }

    function rmfile(path, options, callback) {
        callback(new Error('rmfile: Not Implemented'));
    }

    function readdir(path, options, callback) {
        var prefix = path.substr(1);
        if (prefix.length !== 0 && prefix[prefix.length - 1] !== '/') {
            prefix += '/';
        }

        var meta = {};

        client.list({ prefix: prefix, delimiter: '/' }, function (err, data) {
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

            var children = data.Contents.concat(data.CommonPrefixes);

            meta.stream = stream;
            callback(null, meta);

            var index = 0;
            stream.resume();

            function getNext () {
                if (index === children.length) return done();
                var child = children[index++];
                var left = children.length - index;

                createStatEntry({ root: prefix, child: child }, function (err, entry) {
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

        });
    }

    function stat(path, options, callback) {
        callback(new Error('stat: Not Implemented'));
    }

    function mkdir(path, options, callback) {
        callback(new Error('mkdir: Not Implemented'));
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

        if (options.child.Prefix) {
            entry.name = removeTrailingSlash(options.child.Prefix.substr(options.root.length));
            entry.access = 4 | (true ? 2 : 0); // file.editable == true
            entry.size = 0;
            entry.mtime = (new Date()).valueOf(); // (new Date(file.modifiedDate)).valueOf()
            entry.mime = 'inode/directory';
            callback(null, entry);
        } else if (options.child.Key) {
            entry.name = options.child.Key.substr(options.root.length);
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
