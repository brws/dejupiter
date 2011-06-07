var url = require('url');
var path = require('path');

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

StockList.prototype.save = function(callback) {
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
      for (var t in tables) {
        var table = tables[t];
        if (!data[t]) data[t] = {};
        
        for (var t_attr in table) {
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
  
  if (GLOBAL.options.insert_data) {
    var dals = new Dals('stock', 'root', 'root');
    dals.save(stock, 100, callback);
  } else {
    console.log('Not inserting data. Problem?');
    callback();
  }
};

module.exports = StockList;