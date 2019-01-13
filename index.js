"use strict";

module.exports = function(c, next){
  if(c.use && c.use in ['postgresql']){
    require('./' + c.use).connect(c[c.use], next);
  } else {
    throw "Config references DB-engine that does not exist: " + c.use;
  }
};
