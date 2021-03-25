const Transaction = require("./Transaction");

class DBS {
  constructor(dbs, config) {
    this._dbs = dbs;
    this._config = config;
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
    return new Transaction(this._dbs, col, this);
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
    try {
      return this._dbs.query(q);
    } catch (e) {
      this._log("Error in updateOne:", this._query, this._params, e);
      throw e;
    }
  }

  createUser(user) {}

  convertType(type) {
    switch (type.type) {
      case "int":
        return "integer";
      case "id":
        if (this._config.idsAsBigInt) {
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
        if (this._config.idsAsBigInt) {
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

  async raw(query, params) {
    try {
      return await this._dbs.query(query, params);
    } catch (e) {
      this._log("Error in dbs.raw", query, params, e);
      throw e;
    }
  }

  _log(message, query, params, error) {
    if (this._config.logs === "errors") {
      if (this._config.logParams) {
        console.log(
          message,
          "\nQUERY:\n",
          query,
          "\nPARAMS:\n",
          params,
          "\nERROR:\n",
          error
        );
      } else {
        console.log(message, "\nQUERY:\n", query, "\nERROR:\n", error);
      }
    }
  }
  /* END DBS FUNCTIONS */
}

module.exports = DBS;
