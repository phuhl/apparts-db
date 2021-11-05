import { Pool } from "pg";
import { PGConfig } from "../Config";

import Transaction, { Params, Id } from "./Transaction";

class DBS {
  _dbs: Pool;
  _config: PGConfig;
  _query?: string;
  _params?: any[];

  constructor(dbs: Pool, config: PGConfig) {
    this._dbs = dbs;
    this._config = config;
  }

  /* ID FUNCTIONS */
  newId() {
    return undefined;
  }

  fromId(id: Id) {
    return id;
  }

  toId(id: Id) {
    return id;
  }
  /* END ID FUNCTIONS */

  /* DBS FUNCTIONS */
  collection(col: string) {
    return new Transaction(this._dbs, col, {
      config: this._config,
      log: (...ps) => this._log(...ps),
    });
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
  createCollection(
    name: string,
    indexes: {
      key?: string[];
      name: string;
      unique?: boolean;
      foreign?: { table: string; field: string };
    }[],
    fields: {
      name: string;
      type: string;
      notNull?: boolean;
      default?: string;
    }[],
    prefix?: string
  ) {
    if (prefix) {
      prefix += "_";
    } else {
      prefix = "";
    }
    let q = `CREATE TABLE "${name}" (`;
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
              `("${i.name}") REFERENCES "${i.foreign.table}" ` +
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

  createUser() {}

  convertType(type: { type: string; maxLength?: boolean; auto?: boolean }) {
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

  shutdown(next: () => {}) {
    this._dbs.end(() => {
      next && next();
    });
  }

  async raw(query: string, params: any[]) {
    try {
      return await this._dbs.query(query, params);
    } catch (e) {
      this._log("Error in dbs.raw", query, params, e);
      throw e;
    }
  }

  _log(message: string, query: string, params: Params, error: any) {
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

export default DBS;
