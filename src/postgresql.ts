"use strict";

import { PGConfig } from "./Config";
import { Pool, types as pgTypes } from "pg";
import DBS from "./postgresql/DBS";

let pool: undefined | Pool;

const connectPG = function (
  c: PGConfig,
  next: (error: boolean | any, dbs?: DBS) => void,
  error?: (err: any) => void
) {
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

  pool.on("error", (err) => {
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

export default connectPG;
