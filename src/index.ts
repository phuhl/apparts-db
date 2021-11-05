"use strict";
import connectPG from "./postgresql";
import Config from "./Config";

function connect(c: Config, next: () => {}) {
  if (c.use && ["postgresql"].indexOf(c.use) !== -1) {
    connectPG(c[c.use], next);
  } else {
    throw "Config references DB-engine that does not exist: " + c.use;
  }
}

export default connect;
