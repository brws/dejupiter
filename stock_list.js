var url = require('url');
var path = require('path');
var Autocheck = require('experian');

Dals = require('./dals');

function is_url(s) {
  var regexp = /(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?/;
  return regexp.test(s);
}

function StockList() {
  this.records = [];
}

StockList.prototype.add = function(data) {
  var record = this.find(data.FullRegistration);
  
  if (record == false) {
    this.records.push([data.FullRegistration, data]);
    return true;
  } else {
    return false;
  }
};

StockList.prototype.find = function(registration) {
  for (var i = 0; i < this.records.length; i++) {
    var record = this.records[i];
    if (record[0] == registration) return record[1];
  }
  
  return false;
};

StockList.prototype.remove = function(registration) {
  for (var i = 0; i < this.records.length; i++) {
    var record = this.records[i];
    
    if (record[0] == registration) {
      this.records.splice(i, 1);
      return record[1];
    }
  }
  
  return false;
};

StockList.prototype.length = function() {
  return this.records.length;
};

StockList.prototype.get_experian_data = function(data, callback) {
 var ac = new Autocheck('CASOLUTIONSUAT', 'GZ0XWTYH');
 var async = require('async');
 var dals = new Dals(GLOBAL.mysql_database, GLOBAL.mysql_user, GLOBAL.mysql_pass);
 
 var index = 0;
 
 async.forEachSeries(data, function(item, cb) {
   dals.query(["SELECT * FROM `extra` WHERE stock_id = ?", [item.cars.id]], function(results) {
     if (results.length >= 1 && (!!results[0].co2 && results[0].co2.length || !!results[0].vin && results[0].vin.length)) {
       index++;
       cb();
     } else {
       if (item.cars.registration != 'Motability') {
         ac.request({
           transactiontype: '03',
           vrm: item.cars.registration,
           capid: 1,
         }, function(err, res) {
           if (err) {
             console.log(err.message);
           } else {
             if (!!res.request.mb01) {
               console.log('Got VIN: ' + res.request.mb01.vinserialnumber);
               console.log('Got CO2: ' + res.request.mb01.co2emissions);
               
               data[index].extra.co2 = res.request.mb01.co2emissions;
               data[index].extra.vin = res.request.mb01.vinserialnumber;
             } else {
               console.log(res.request);
             }
             
             if (!!res.request.mb34) {
               console.log('Got CAPID: ' + res.request.mb34.capid);
               data[index].extra.capid = res.request.mb34.capid;
             } else {
               console.log(res.request);
             }
      
             index++;
             cb();
           }
         });
       } else {
         index++
         cb();
       }
     }
   });
 }, function(err) {
   if (!err) callback(data);
   if (err) console.log(err);
 });
 
 /*

  ac.request({
    capcode: 1,
    capid: 1,
    transactiontype: '03',
    vrm: 'CA09PMY'
  }, function(err, res) {
    if (err) {
      console.log(err.message);
    } else {
      console.log('Result: ' + require('util').inspect(res, true, 100));
    }
  });*/
};

StockList.prototype.save = function(callback) {
  var self = this;
  var stock = [];
  
  for (var i = 0; i < this.records.length; i++) {
    var tables = {
      cars: {
        Vehicle_ID: 'id',
        Feed_ID: 'location_id',
        FullRegistration: 'registration',
        Colour: 'colour',
        FuelType: 'fuel',
        Year: 'registration_year',
        Mileage: 'mileage',
        Bodytype: 'body_type',
        Doors: 'doors',
        Make: 'make',
        Model: 'model',
        Variant: 'variant',
        EngineSize: 'engine_size',
        Price: 'price',
        Transmission: 'transmission',
        Description: 'vehicle_description',
        Used: 'used',
        Interior_Colour_Material: 'interior_colour_material'
      },
      extra: {
        Vehicle_ID: 'stock_id',
        Cap_ID: 'capid',
        VIN: 'vin'
      },
    };
    
    var record = this.records[i];
    var registration = record[0];
    var vehicle = record[1];
    var data = {images: [], options: []};
    
    for (var attr in vehicle) {
      if (vehicle.hasOwnProperty(attr)) {
        for (var t in tables) {
          if (tables.hasOwnProperty(t)) {
            var table = tables[t];
            if (!data[t]) data[t] = {};
            
            for (var t_attr in table) {
              if (table.hasOwnProperty(t_attr)) {
                if (attr.toLowerCase() == t_attr.toLowerCase()) {
                  if (vehicle[attr] !== undefined
                      && JSON.stringify(vehicle[attr]) !== '{}'
                      && vehicle[attr] !== '"') {
                    data[t][table[t_attr]] = vehicle[attr];
                  } else {
                    data[t][table[t_attr]] = 'NULL';
                  }
                }
              }
            }
          }
        }
      }
    }
    
    for (var table in data) {
      var keys = Object.keys(data[table]);
      
      for (var k in tables[table]) {
        var key = tables[table][k];
        
        if (keys.indexOf(key) == -1) {
          data[table][key] = null;
        }
      }
    }
    
    data.cars.id = data.cars.id.replace('AETV', '');
    data.extra.stock_id = data.extra.stock_id.replace('AETV', '');
    data.cars.location_id = data.cars.location_id.replace('AETA', '');
    
    var images = vehicle.PictureRefs.split(',');
    var options = vehicle.Options.split(',');
    
    images.forEach(function(image) {
      if (is_url(image)) {
        var image_url = url.parse(image);
        image = path.basename(image_url.pathname);
      }
      
      if (image != '"' && image.length > 1) {
        data.images.push({stock_id: data.cars.id, image: image});
      }
    });
    
    options.forEach(function(option) {
      data.options.push({stock_id: data.cars.id, option: option});
    });
    
    stock.push(data);
  }
  
  console.log('Getting CAP Data');
  
  self.get_experian_data(stock, function(capdata) {
    console.log('Got CAP Data');
    stock = capdata;
    
    if (GLOBAL.options.insert_data) {
      GLOBAL.statistics.records = stock.length;
      var dals = new Dals(GLOBAL.mysql_database, GLOBAL.mysql_user, GLOBAL.mysql_pass);
      dals.save(stock, 100, callback);
    } else {
      console.log('Not inserting data. Problem?');
      callback();
    }
  });
};

module.exports = StockList;