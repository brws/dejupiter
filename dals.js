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
    self.mysql.user = self.user;
    self.mysql.password = self.pass;
    
    self.mysql.connect(function () {
      self.mysql.query('use `'+self.database+'`;', function(err) {
        if (err) {
          console.log('err: ' + err);
          process.exit();
        }
        console.log('connected to mysql');
        self.connected = true;
        self.save(data, records, callback);
      });
    });
  } else {
    if (data.length > 0) {
      var set = data.splice(0, records);
      console.log('Inserting ' + set.length + ' record(s)');
      
      self.insert(set, function() {
        self.save(data, records, callback);
      });
    } else {
      callback();
    }
  }
}

module.exports = Dals;