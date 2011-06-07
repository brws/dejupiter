var images = Array.prototype.slice.call(process.argv, 3);

var async = require('async');
var fs = require('fs');
var http = require('http');
var parse   = require('url').parse;
var path = require('path');

function save_image(to, data, callback) {
  fs.open(to, 'w', function(err, fd) {
    if (err) {
      callback(err);
    } else {
      fs.write(fd, data, 0, data.length, 0, function(err, written, buffer) {
        if (err) {
          callback(err);
        } else {
          callback(false, written);
        }
      });
    }
  });
}

function download_image(url, callback) {
  process.stdout.write('getting ' + url + '                            \r');
  var urlobj = parse(url);
  
  var pathname = urlobj.pathname;
  var file = pathname.split('/');
  var filename = file[file.length-1];
  
  path.exists(process.argv[2] + '/' + filename, function(exists) {
    if (exists) {
      process.stdout.write(process.argv[2] + '/' + filename + ' exists                            \r');
      callback();
    } else {
      var request = http
                    .createClient(80, urlobj.host)
                    .request('GET', urlobj.pathname, {
                      'host': urlobj.hostname
                    });
      
      request.on('response', function(res) {
        var body = '';
        
        var total = res.headers['content-length'];
        
        res.setEncoding('binary');
        
        res.on('end', function () {
          var image = new Buffer(body, 'binary');
          var path = process.argv[2] + '/' + filename;
          
          save_image(path, image, function(err, written) {
            if (err) {
              callback(err);
            } else {
              process.stdout.write('wrote ' + written + ' byte(s) to ' + path + '                            \r\n');
              callback();
            }
          });
        });
        
        res.on('data', function (chunk) {
          var perc = Math.round((body.length / total) * 100) + '%';
          if (res.statusCode == 200) body += chunk;
          process.stdout.write('getting ' + perc + ' of ' + filename + '                            \r');
        });
      });
      
      request.end();
    }
  });
}

async.forEach(images, download_image, function(err) {
  if (err) {
    console.log(err.message);
    process.exit(1);
  } else {
    process.exit(0);
  }
});