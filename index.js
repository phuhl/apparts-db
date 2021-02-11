"use strict";
const postgres = require("./postgresql");
module.exports = function (c, next) {
  if (c.use && ["postgresql"].indexOf(c.use) !== -1) {
    postgres.connect(c[c.use], next);
  } else {
    throw "Config references DB-engine that does not exist: " + c.use;
  }
};
