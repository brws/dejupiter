var async = require('async');

function Dals(database, user, pass) {
  this.mysql = new require('mysql').Client();
  this.database = database;
  this.user = user;
  this.pass = pass;
  this.connected = false;
}

/**
 * Inserts the data in MySQL then callsback when the query's all fine
 * and dandy.
 */

Dals.prototype.insert = function(set, callback) {
  var self = this;
  
  var tables = Object.keys(set[0]);
  var inserts = {};
  
  tables.forEach(function(table) {inserts[table] = []});
  
  set.forEach(function(record, index) {
    for (var table in record) {
      var data = record[table];
      switch (table) {
        case "options":
        case "images":
          data.forEach(function(dat) {
            inserts[table].push(dat);
          });
        break;
        default:
          inserts[table].push(data);
        break;
      }
    }
  });
  
  async.forEach(tables, function(table, cb) {
    var query = self.insert_rows(inserts[table], table);
    self.mysql.query(query[0], query[1], cb);
  }, callback);
};

Dals.prototype.insert_rows = function(object, table) {
  var query = 'INSERT INTO ' + table + ' (';
  
  var cols = [], vals = [], qs = [], valp = [];

  for (var column in object[0]) {
    cols.push('`'+column+'`');
    qs.push('?');
  }
  
  query += cols.join(', ') + ') VALUES ';
  
  for (var i = 0; i < object.length; i++) {
    valp.push('(' + qs.join(', ') + ')');
    
    for (var column in object[0]) {
      var value = (""+object[i][column]).trim();
      if (value.toLowerCase() == "null" || value == 'undefined' || value == undefined) {
        vals.push(null);
      } else {
        vals.push(value);
      }
      
    }
  }
  
  query += valp.join(', ') + ';';
  
  return [query, vals];
};

Dals.prototype.query_callback = function(err, results, callback) {  
  if (err) {
    console.log('Query Error: ' + err);
    callback(err);
  } else {
    if (results.length > 0) {
      callback(results);
    } else {
      callback();
    }
  }
};

Dals.prototype.query = function(query, callback) {
  var self = this;
  
  console.log('Executing Query ' + query[0]);
  
  this.connect(function() {
    console.log('Continuing...');
    if (query.length == 1) {
      self.mysql.query(query[0], function(err, results) {
        if (!results) results = [];
        self.query_callback(err, results, callback);
      });
    } else {
      self.mysql.query(query[0], query[1], function(err, results) {
        if (!results) results = [];
        self.query_callback(err, results, callback);
      });
    }
  });
};

Dals.prototype.connect = function(callback) {
  var self = this;
  
  if (self.connected == true) {
    callback();
  } else {
    self.mysql.user = self.user;
    self.mysql.password = self.pass;
    self.mysql.connect(function () {
      self.mysql.query('use `'+self.database+'`;', function(err) {
        if (err) {
          console.log('Connection Error: ' + err);
          process.exit();
        } else {
          self.connected = true;
          console.log('Connected to MySQL');
          callback();
        }
      });
    });
  }
};

/**
 * Saves data in record inserts per query.
 * 
 * Warning: This is a recursive function. Consequences will never be the same.
 * 
 * SAVE(DATA, RECORDS, CALLBACK) -> (INSERT [records] elements,
 * SPLICE [records] elements from DATA)
 * -> SAVE(DATA, RECORDS, CALLBACK) ==> CALLBACK
 */

Dals.prototype.save = function(data, records, callback) {
  var self = this;
  
  if (self.connected == false) {
    this.connect(function() {
      self.connected = true;
      self.save(data, records, callback);
    });
  } else {
    if (data.length > 0) {
      var set = data.splice(0, records);      
      self.insert(set, function() {
        self.save(data, records, callback);
      });
    } else {
      callback();
    }
  }
}

module.exports = Dals;