// modules

var csv     = require('ya-csv');
var fs      = require('fs');
var path    = require('path');
var spawn   = require('child_process').spawn;
var async   = require('async');

// classes

StockList = require('./stock_list');

// variables (global to file)

var max_children = 4;
var children = 0;
var slaves = [];
var todownload = [];
var tomove = [];
var process_files = [];
var duplicate_count = {};
var records = new StockList();
var imports = [];
var types   = [];

// options global to process

GLOBAL.options = {
  use_portfolio: true,
  use_progress: true,
  use_motors: true,
  download_images: true,
  move_images: true,
  generate_thumbnails: true,
  insert_data: true
};

GLOBAL.mysql_user = 'root';
GLOBAL.mysql_pass = 'root';
GLOBAL.mysql_database = 'stock';

GLOBAL.statistics = {};
GLOBAL.statistics.duplicates = [];
GLOBAL.statistics.errors = [];

var argv = process.argv;

for (var i = 0; i < argv.length; i++) {
  if (argv[i].substr(0,4) == "--no") {
    var option = argv[i].substr(5).replace("-", "_");
    
    if (options[option]) {
      options[option] = false;
    }
  }
}

if (options.use_portfolio) {
  imports.push('portfolio');
  types.push('txt');
}

if (options.use_motors) {
  imports.push('motors');
  types.push('csv');
}

// functions (global to file)

function is_url(s) {
  var regexp = /(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?/;
  return regexp.test(s);
}

function ucfirst(str) {
  str += '';
  var f = str.charAt(0).toUpperCase();
  return f + str.substr(1);
}

// program logic

/**
 *  This function recursively calls itself to spawn up to [max_children] worker processes
 *  unlike the generate function below, which uses the async module's queue functionality
 *  to process a large amount of parallel workers.
 * 
 *  These workers then download the image from the URLs passed, up to six at a time per
 *  process. In theory, only [max_children]*6 URLs are downloaded at any one time.
 * 
 *  [max_children] should be set to 2*cpu cores.
 */
function download(images, callback) {
  if (children < max_children) {
    var options = [];
    
    if (images.length == 0) {
      if (children == 0) {
        callback();
      }
    } else {
      var left = images.splice(0, 6);
          
      options = [__dirname + '/slave.js', __dirname + '/images/f'];
      options = options.concat(left);
        
      children++;
      
      var slave = spawn('/usr/local/bin/node', options, {
        cwd: __dirname,
        env: process.env,
        customFds: [-1, -1, -1],
        setsid: false
      });
      
      download(images, callback);
      
      slave.on('exit', function(code) {
        if (code == 0) {
          children--;
          download(images, callback);
        } else {
          children--;
        }
      });
    }
  }
}

/**
 *  This function checks the folder for images and moves them to images/f if they are contained in the stock record.
 */
function move_files(files, callback) {
  async.forEachSeries(files, function(file, cb) {
    path.exists(__dirname + '/images/f/' + file[1], function(exists) {
      if (exists) {
        cb();
      } else {
        fs.rename(__dirname + '/' + file[0] + '/' + file[1], __dirname + '/images/f/' + file[1], cb);
      }
    });
    
  }, callback);
}

/**
 *  This function parses progress' stock feed from XML and puts it in line with the standard AutoEdit CSV export.
 */

function parse_progress(data, callback) {
  var xmljson = require('xmljson');
  var parser = new xmljson();  
  var transform = {'Feed_Id': 'FeedID', 'Vehicle_ID': 'UsedVehicleID', 'FullRegistration': 'Registration', 'FuelType': 'Fuel', 'Year': 'RegistrationYear', 'Bodytype': 'BodyType', 'Cap_Id': 'CAPID', 'PictureRefs': 'Images', 'Description': 'Vehicle_Description'};
  var turboedit = ["Feed_Id","Vehicle_ID","FullRegistration","Colour","FuelType","Year","Mileage","Bodytype","Doors","Make","Model","Variant","EngineSize","Price","Transmission","PictureRefs","ServiceHistory","PreviousOwner","Category","FourWheelDrive","Options","Comments","New","Used","Site","Origin","V5","Description","Condition","ExDemo","FranchiseApproved","TradePrice","TradePriceExtra","ServiceHistoryText","Cap_Id","VIN","Interior_Colour_Material"];
  
  duplicate_count['progress'] = 0; 
  
  parser.on('end', function(result) {
    for (var v in result.Vehicle) {
      var vehicle = result.Vehicle[v];
      var d = {};
      
      for (var t in turboedit) {
        var te = turboedit[t];
        var vh = !!transform[te] ? transform[te] : te;
        d[te] = vehicle[vh];
      }
      
      if (d['Options']) {
        try {
          d['Options'] = d['Options']['ITEM'].join(',');
        } catch (e) {
          d['Options'] = '';
        }
      } else {
        d['Options'] = '';
      }
      
      if (d['PictureRefs']) {
        try {
          d['PictureRefs'] = d['PictureRefs']['Image'].join(',');
        } catch (e) {
          d['PictureRefs'] = '';
        }
      } else {
        d['PictureRefs'] = '';
      }
      
      var record_added = records.add(d);
      
      if (record_added[0] == false) {
        console.log('Tried to add a duplicate car: ' + record_added[1].FullRegistration);
        GLOBAL.statistics.duplicates.push(record_added[1].FullRegistration);
      }
      
      var picrefs = d.PictureRefs.split(',');
      
      for (var i = 0; i < picrefs.length; i++) {
        if (is_url(picrefs[i])) {
          todownload.push(picrefs[i]);
        } else {
          if (picrefs[i] !== '"')
          tomove.push(['progress', picrefs[i]]);
        }
      }
    }
    
    callback();
  });
  
  parser.parse(data);
  parser.close();
}

/**
 *  This function reads through the CSVs given and feeds the image locations through to the various
 *  image handling functions (either to move them or to download them).
 */
function consume(file, callback) {
  var reader = csv.createCsvFileReader(__dirname + '/' + file[0] + '/' + file[1], {columnsFromHeader: true});
  reader.on('data', function(data) {
    
    var record_added = records.add(data);
    
    if (record_added[0] == false) {
      console.log('Tried to add a duplicate car: ' + record_added[1].FullRegistration);
      GLOBAL.statistics.duplicates.push(record_added[1].FullRegistration);
    }
    
    try {
      var picrefs = data.PictureRefs.split(',');
    } catch(e) {
      console.log(file[0]);
      console.log(data);
      process.exit();
    }
    
    for (var i = 0; i < picrefs.length; i++) {
      if (is_url(picrefs[i])) {
        todownload.push(picrefs[i]);
      } else {
        if (picrefs[i] !== '"')
        tomove.push([file[0], picrefs[i]]);
      }
    }
  });
  
  reader.on('end', function() {
    console.log('Loading ' + records.length() + ' stock record(s)');
    callback();
  });
}

/**
 *  This function does the actual grunt work of resizing the images with the imagemagick module.
 */
function copy_resize_image(filename, callback, size) {
  var dest = __dirname + '/images/'+size+'/';
    
  var sizes = {
    't': {width: 140, height: 90},
    's': {width: 80, height: 57},
    'l': {width: 290, height: 190},
  };
  
  path.exists(dest + '/' + path.basename(filename), function(exists) {
    if (exists) {
      callback(null, filename);
    } else {
      im.resize({
        srcPath: filename,
        dstPath: dest + '/' + path.basename(filename),
        width: sizes[size].width,
        height: sizes[size].height
      }, function() {
        callback(null, filename);
      });
    }
  });
}

/**
 *  This function is an asynchronus function which takes task.filename and generates three thumbnails
 *  and saves said thumbnails to various directories.
 */
function generate(task, callback) {
  async.parallel([
    function(cb) { // t
      copy_resize_image(task.filename, cb, 't');
    },
    function(cb) { // s
      copy_resize_image(task.filename, cb, 's');
    },
    function(cb) { // l
      copy_resize_image(task.filename, cb, 'l');
    },
  ], function(err, results) {
    if (err) {
      callback(err);
    } else {
      callback();
    }
  });
}

function queue(files, callback) {
  GLOBAL.statistics.thumbnails = files.length;
  GLOBAL.statistics.ratio = Math.round((files.length / records.length()) * 100);
  console.log('Queueing ' + files.length + ' file(s) for thumbnail generation');
  console.log('That\'s an image ratio of ' + GLOBAL.statistics.ratio + '% by the way.');
  
  var q = async.queue(generate, 20);
  
  for (var i = 0; i < files.length; i++) {
    var file = files[i];
    
    q.push({filename: __dirname + '/images/f/' + file});
  }
  
  q.drain = function() {
    callback();
  };
  
  if (files.length == 0) callback();
}

function unzip(file, directory, callback) {
  var unzip = spawn('unzip', ['-o', file, '-d', __dirname + '/' + directory]);
  
  var files = [];
  
  unzip.stdout.on('data', function(data) {
    var data = data.toString();
    if (data.length > 2) {
      var match = /inflating: (\S+)/mig.exec(data);
      
      if (match != null) {
        files.push(match[1]);
      }
    }
  });
  
  unzip.stderr.on('data', function(data) {
    console.log(data.toString());
    unzip.kill();
  });
  
  unzip.on('exit', function(code) {
    if (code !== 0) {
      callback({message: 'Unzipping failed'});
    } else {
      callback();
    }
  });
}

GLOBAL.statistics.start = new Date().getTime();

async.series([
  function(callback) {
    if (!options.use_portfolio) {
      callback();
    } else {
      console.log('Unzipping portfolio\'s feed.');
      unzip(__dirname + '/portfolio/*.zip', 'portfolio', callback);
    }
  },
  
  function(callback) {
    imports.forEach(function(dir, index) {
      fs.readdir(__dirname + '/' + dir, function(err, files) {
        files.forEach(function(file) {
          if (file.substr(-3) == types[index]) {
            process_files.push([dir, file]);
            
            if (process_files.length == imports.length) callback(null);
          }
        });
      });
    });
    
    if (imports.length == 0) callback();
  },
  
  function(callback) {
    if (process_files.length == 0) {
      callback();
    } else {
      console.log('Consuming ' + process_files.length + ' feeds(s)');
      async.forEachSeries(process_files, consume, callback);
    }
  },
  
  function(callback) {
    if (!options.use_progress) {
      callback();
    } else {
      console.log('Beligerantly feeding in Progress\' stock list because we have to.. for now.');
      console.log('Just unzipping the behemoth as we speak..');
      unzip(__dirname + '/progress/*.zip', 'progress', callback);
    }
  },
  
  function(callback) {
    console.log('Done. Feeding it in.');
    if (!options.use_progress) {
      callback();
    } else {
      fs.readFile(__dirname + '/progress/Progress.xml', 'utf8', function(err, data) {
        if (err) {
          callback(err);
        } else {
          parse_progress(data, callback);
        }
      });
    }
  },
  
  function(callback) {
    if (!options.download_images) {
      callback();
    } else {
      GLOBAL.statistics.download_count = todownload.length;
      console.log('Downloading ' + todownload.length + ' stock photo(s)');
      download(todownload, callback);
    }
  },
  
  function(callback) {
    if (!options.move_images) {
      callback();
    } else {
      GLOBAL.statistics.move_count = tomove.length;
      console.log('Moving ' + tomove.length + ' stock photo(s)');
      move_files(tomove, callback);
    }
  },
  
  function(callback) {
    if (!options.generate_thumbnails) {
      callback();
    } else {
      fs.readdir(__dirname + '/images/f', function(err, files) {
        if (err) {
          callback(err);
        } else {
          queue(files, callback);
        }
      });
    }
  },
  
  function(callback) { // clear all images/options
    Dals = require('./dals');
    
    var dals = new Dals(GLOBAL.mysql_database, GLOBAL.mysql_user, GLOBAL.mysql_pass);
    
    dals.query(['TRUNCATE `extra`;'], function() {
      dals.query(['TRUNCATE `options`;'], function() {
        dals.query(['TRUNCATE `images`;'], callback);
      });
    });
  },
  
  function(callback) {
    console.log('Inserting records');
    records.save(callback);
  },
  
  function(callback) {
    console.log('Inserting Locations Crap');
    
    Dals = require('./dals');
    
    locations = [];
    
    locations.push({
      'id': 70186,
      'name': 'Bath Volkswagen',
      'telephone': '01225 562079',
      'address': ['Locksbrook Road', '', 'Bath', 'BA1 3EU'],
      'geocode': '51.382426,-2.387236'
    });
  
    locations.push({
      'id': 102950,
      'name': 'Capitol Volkswagen',
      'telephone': '01685 350077',
      'address': ['Pentrebach Road', 'Merthyr Tydfil', 'Mid Glamorgan', 'CF48 1YA'],
      'geocode': '51.734075,-3.370167'
    });
  
    locations.push({
      'id': 15843,
      'name': 'Newport Ford',
      'telephone': '01633 730752',
      'address': ['Leeway Industrial Estate', 'Leeway', 'Newport', 'NP19 4TS'],
    });
  
    locations.push({
      'id': 11633,
      'name': 'Mon Motors Chippenham',
      'telephone': '01249 667765',
      'address': ['Methuen Park', 'Chippenham', 'Wiltshire', 'SN14 0GX'],
    });
  
    locations.push({
      'id': 25708,
      'name': 'Brecon Ford',
      'telephone': '01874 622401',
      'address': ['Leeway Industrial Estate', 'Leeway', 'Newport', 'NP19 4TS'],
    });
  
    locations.push({
      'id': 30835,
      'name': 'Cwmbran Ford',
      'telephone': '01633 730746',
      'address': ['Leeway Industrial Estate', 'Leeway', 'Newport', 'NP19 4TS'],
    });
  
    locations.push({
      'id': 41798,
      'name': 'Bath Audi',
      'telephone': '01761 441352',
      'address': ['Leeway Industrial Estate', 'Leeway', 'Newport', 'NP19 4TS'],
    });
  
    locations.push({
      'id': 49286,
      'name': 'Chepstow Ford',
      'telephone': '01291 661343',
      'address': ['Leeway Industrial Estate', 'Leeway', 'Newport', 'NP19 4TS'],
    });
  
    locations.push({
      'id': 115821,
      'name': 'Bristol Audi',
      'telephone': '0117 314 9308',
      'address': ['Leeway Industrial Estate', 'Leeway', 'Newport', 'NP19 4TS'],
    });
  
    locations.push({
      'id': 1026728,
      'name': 'Cardiff Audi',
      'telephone': '02920 609087',
      'address': ['Leeway Industrial Estate', 'Leeway', 'Newport', 'NP19 4TS'],
    });
  
    locations.push({
      'id': 1598290,
      'name': 'Capitol Skoda',
      'telephone': '01633 730742',
      'address': ['Leeway Industrial Estate', 'Leeway', 'Newport', 'NP19 4TS'],
    });
    
    var dals = new Dals(GLOBAL.mysql_database, GLOBAL.mysql_user, GLOBAL.mysql_pass);
    var query = dals.insert_rows(locations, 'locations');
    
    dals.query(query, function(err) {
      if (err) {
        GLOBAL.statistics.errors.push(err.message);
        callback();
      } else {
        callback();
      }
    });
  },
  
  function(callback) {
    GLOBAL.statistics.end = new Date().getTime();
    
    var mailer = require('mailer');
    
    mailer.send({
      ssl: true,
      host: 'auth.smtp.1and1.co.uk',
      port: 465,
      domain: 'alpha.monmotors.com',
      to: 'robin@bluerewards.co.uk, gareth.rosser@monmotors.com',
      from: 'root@alpha.monmotors.com',
      subject: 'Stock System Import - ' + new Date().toDateString(),
      template: __dirname + '/email_template.txt',
      data: {
        duplicates: GLOBAL.statistics.duplicates.join('\n'),
        errors: GLOBAL.statistics.errors.join('\n'),
        time: ((GLOBAL.statistics.end - GLOBAL.statistics.start) / 1000) / 60,
        records: GLOBAL.statistics.records+0,
        moved: GLOBAL.statistics.move_count+0,
        downloaded: GLOBAL.statistics.download_count+0,
        date: new Date().toDateString()
      },
      authentication: "login",
      username: "root@alpha.monmotors.com",
      password: "capitol99"
    }, function(err, result) {
      if (err) console.log('Couldn\'t send email. ' + err);
      callback();
    });
  }
], function(err) {
  if (err) {
    console.log('\nerr: ' + err.message + '\r\n');
    process.exit(1);
  } else {
    console.log('\nOh. Done already? Yep.' + '\r\n');
    process.exit(0);
  }
});