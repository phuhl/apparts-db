"use strict";

const { Pool, types: pgTypes } = require("pg");
const DBS = require("./postgresql/DBS.js");

let pool;

module.exports.connect = function (c, next, error) {
  pool = new Pool({
    host: c.host,
    port: c.port,
    user: c.user,
    password: c.pw,
    database: c.db,
    max: c.maxPoolSize || 10,
    connectionTimeoutMillis: c.connectionTimeoutMillis || 0,
    idleTimeoutMillis: c.idleTimeoutMillis || 10000,
  });

  pool.on("error", (err, client) => {
    // What to do?
    console.log(
      `Postgres DB-connection failed for host ${c.host}:${c.port},` +
        ` ${c.user}@${c.db} with ERROR: ${err}`
    );
    error && error(err);
  });

  if (c.bigIntAsNumber) {
    // Return Bigint and stuff as number, not as string
    pgTypes.setTypeParser(20, function (val) {
      return parseInt(val);
    });
  }

  // Test connection
  pool
    .connect()
    .then((client) => {
      client.release();
      next(false, new DBS(pool, c));
    })
    .catch((e) => {
      next(e);
    });
};
