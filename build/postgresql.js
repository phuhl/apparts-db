"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const pg_1 = require("pg");
const DBS_1 = __importDefault(require("./postgresql/DBS"));
let pool;
const connectPG = function (c, next, error) {
    pool = new pg_1.Pool({
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
        console.log(`Postgres DB-connection failed for host ${c.host}:${c.port},` +
            ` ${c.user}@${c.db} with ERROR: ${err}`);
        error && error(err);
    });
    if (c.bigIntAsNumber) {
        pg_1.types.setTypeParser(20, function (val) {
            return parseInt(val);
        });
    }
    pool
        .connect()
        .then((client) => {
        client.release();
        next(false, new DBS_1.default(pool, c));
    })
        .catch((e) => {
        next(e);
    });
};
exports.default = connectPG;
//# sourceMappingURL=postgresql.js.map