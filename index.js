"use strict";

module.exports = function(c, next){
  if(c.use && ['postgresql'].indexOf(c.use) !== -1){
    require('./' + c.use).connect(c[c.use], next);
  } else {
    throw "Config references DB-engine that does not exist: " + c.use;
  }
};
