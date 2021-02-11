"use strict";

const { Pool, types: pgTypes } = require("pg");

let pool;
let idsAsBigInt = false;

class DBS {
  constructor(dbs) {
    this._dbs = dbs;
  }

  /* ID FUNCTIONS */
  newId() {
    return undefined;
  }

  fromId(id) {
    return id;
  }

  toId(id) {
    return id;
  }
  /* END ID FUNCTIONS */

  /* DBS FUNCTIONS */
  collection(col) {
    return new Transaction(this._dbs, col);
  }

  /**
   *
   * @param string name
   * @param [{}] indexes with { name: <field>, key: [<key1>, ...] }
   *                       or { name: <field>, unique: true }
   *                       or { name: <field>, foreign:
   *                                             { table: <table>,
   *                                               field: <field>} }
   * @param [{}] fields with { name: <name>, type: <type>, notNull:
   *                           <true/false>, default: <defaultVal> }
   * @returns Promise
   */
  createCollection(name, indexes, fields, prefix) {
    if (prefix) {
      prefix += "_";
    } else {
      prefix = "";
    }
    let q = `CREATE TABLE public."${name}" (`;
    q += []
      .concat(
        // fields
        fields.map((f) => {
          let res = `"${f.name}" ${f.type}`;
          if (f.notNull) {
            res += " NOT NULL";
          }
          if (f.default !== undefined) {
            res += " DEFAULT " + f.default;
          }
          return res;
        }),
        // constraints
        indexes
          .filter((i) => i.key)
          .map(
            (i) =>
              `CONSTRAINT "${name}_${i.name}_pkey" PRIMARY KEY (` +
              i.key.map((k) => `"${k}"`).join(",") +
              ")"
          ),
        indexes
          .filter((i) => i.unique)
          .map((i) => `CONSTRAINT "${name}_${i.name}_u" UNIQUE ("${i.name}")`),
        indexes
          .filter((i) => i.foreign)
          .map(
            (i) =>
              `CONSTRAINT "${name}_${i.name}_fkey" FOREIGN KEY ` +
              `("${i.name}") REFERENCES public."${i.foreign.table}" ` +
              `(${i.foreign.field}) MATCH SIMPLE`
          )
      )
      .join(",");
    // with
    q += ") WITH ( OIDS = FALSE )";
    return this._dbs.query(q);
  }

  createUser(user) {}

  convertType(type) {
    switch (type.type) {
      case "int":
        return "integer";
      case "id":
        if (idsAsBigInt) {
          return type.auto ? "bigserial" : "bigint";
        } else {
          return type.auto ? "serial" : "integer";
        }
      case "bool":
        return "boolean";
      case "float":
        return "double precision";
      case "/":
      case "string":
      case "hex":
      case "base64":
      case "email":
      case "password":
        return type.maxLength ? `varchar(${type.maxLength})` : "text";
      case "time":
        return "bigint";
      case "array_time":
        return "bigint[]";
      case "array_bigint":
        return "bigint[]";
      case "array_id":
        if (idsAsBigInt) {
          return "bigint[]";
        } else {
          return "integer[]";
        }
    }
    throw new Error("ERROR: Type not found: " + JSON.stringify(type));
  }

  shutdown(next) {
    this._dbs.end(() => {
      next && next();
    });
  }

  raw(query, params) {
    return this._dbs.query(query, params);
  }

  /* END DBS FUNCTIONS */
}

class Transaction {
  constructor(dbs, col) {
    this._dbs = dbs;
    this._table = col;
    this._counter = 1;
  }

  find(params, limit, offset, order) {
    let q = `SELECT * FROM public."${this._table}" `;
    let newVals = [];
    q += this._buildWhere(params, newVals);
    if (order) {
      q += order.map((arr) => ` ORDER BY ${arr.key} ${arr.dir}`);
    }
    if (limit) {
      q += ` LIMIT $${this._counter++}`;
      newVals.push(limit);
      if (offset) {
        q += ` OFFSET $${this._counter++}`;
        newVals.push(offset);
      }
    }
    this._query = q;
    this._params = newVals;
    return this;
  }

  _buildWhere(params, newVals) {
    let keys = Object.keys(params);
    if (keys.length === 0) {
      return "";
    }
    let vals = keys.map((key) => params[key]);
    return (
      "WHERE " +
      keys
        .map((key, i) => {
          if (typeof vals[i] !== "object") {
            newVals.push(vals[i]);
            return `"${key}" = $${this._counter++}`;
          } else {
            let op = vals[i].op;
            return this._decideOperator(key, op, vals[i].val, newVals);
          }
        })
        .join(" AND ")
    );
  }

  _decideOperator(key, op, val, newVals) {
    switch (op) {
      case "any":
        newVals.push(val);
        return `$${this._counter++} = ANY("${key}")`;
      case "in":
        val.forEach((v) => newVals.push(v));
        return (
          `"${key}" IN (` +
          val.map((id, i) => `$${this._counter++}`).join(",") +
          ")"
        );
      case "lte":
        newVals.push(val);
        return `"${key}" <= $${this._counter++}`;
      case "lt":
        newVals.push(val);
        return `"${key}" < $${this._counter++}`;
      case "gte":
        newVals.push(val);
        return `"${key}" >= $${this._counter++}`;
      case "gt":
        newVals.push(val);
        return `"${key}" > $${this._counter++}`;
      case "like":
        newVals.push(val);
        return `"${key}" LIKE $${this._counter++}`;
      case "and":
        return val
          .map((v) => this._decideOperator(key, v.op, v.val, newVals))
          .join(" AND ");
      default:
        throw new Error("ERROR, operator not implemented: " + op);
    }
  }

  findById(id, limit, offset) {
    return this.find(id, limit, offset);
  }

  findByIds(ids, limit, offset) {
    Object.keys(ids).forEach((key) => {
      if (Array.isArray(ids[key])) {
        ids[key] = { op: "in", val: ids[key] };
      }
    });
    return this.find(ids, limit, offset);
  }

  toArray() {
    this._result = this._dbs.query(this._query, this._params);

    return this._result.then((res) => {
      return Promise.resolve(res.rows);
    });
  }

  insert(content, returning = ["id"]) {
    let q = `INSERT INTO public."${this._table}" `;
    let keys = Object.keys(content[0]);
    q += "(" + keys.map((key) => `"${key}"`).join(",") + ")";
    q += " VALUES ";
    q += content
      .map(
        (c, i) =>
          "(" +
          keys.map((k, j) => `$${i * keys.length + (j + 1)}`).join(",") +
          ")"
      )
      .join(",");
    if (returning && returning.length > 0) {
      q += " RETURNING " + returning.join(",");
    }
    return this._dbs
      .query(
        q,
        [].concat.apply(
          [],
          content.map((c) =>
            keys.map((k) =>
              Array.isArray(c[k]) ? "{" + c[k].join(",") + "}" : c[k]
            )
          )
        )
      )
      .then((res) => {
        return Promise.resolve(res.rows);
      })
      .catch((err) => {
        if (err.code === "23505") {
          return Promise.reject({
            msg: "ERROR, tried to insert, not unique",
            _code: 1,
          });
        } else {
          return Promise.reject(err);
        }
      });
  }

  updateOne(filter, c) {
    let q = `UPDATE public."${this._table}" SET `;
    let counter = 1;
    let keys = Object.keys(c);
    q += "(" + keys.map((k) => `"${k}"`).join(",") + ") = ";
    q += "(" + keys.map((k) => `$${counter++}`).join(",") + ")";
    q += " WHERE ";
    q += Object.keys(filter)
      .map((key, i) => `"${key}" = $${counter + i}`)
      .join(" AND ");
    let vals = keys
      .map((k) => (Array.isArray(c[k]) ? "{" + c[k].join(",") + "}" : c[k]))
      .concat(Object.keys(filter).map((key) => filter[key]));
    return this._dbs.query(q, vals);
  }

  remove(params) {
    let q = `DELETE FROM public."${this._table}" `;
    //    let keys = Object.keys(params);
    let newVals = [];
    q += this._buildWhere(params, newVals);
    //    q += keys.map((key, i) => `"${key}" = $${i + 1}` ).join(' AND ');
    //    let vals = keys.map(key => params[key]);
    return this._dbs.query(q, newVals).catch((err) => {
      if (err.code === "23503") {
        return Promise.reject({
          msg: "ERROR, tried to remove item that is still a reference",
          _code: 2,
        });
      } else {
        return Promise.reject(err);
      }
    });
  }

  drop() {
    let q = `DROP TABLE public."${this._table}"`;
    return this._dbs.query(q);
  }
}

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

  if (c.idsAsBigInt) {
    idsAsBigInt = true;
  }

  // Test connection
  pool
    .connect()
    .then((client) => {
      client.release();
      next(false, new DBS(pool));
    })
    .catch((e) => {
      next(e);
    });
};
