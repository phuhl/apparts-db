"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const Transaction_1 = __importDefault(require("./Transaction"));
class DBS {
    constructor(dbs, config) {
        this._dbs = dbs;
        this._config = config;
    }
    newId() {
        return undefined;
    }
    fromId(id) {
        return id;
    }
    toId(id) {
        return id;
    }
    collection(col) {
        return new Transaction_1.default(this._dbs, col, {
            config: this._config,
            log: (...ps) => this._log(...ps),
        });
    }
    createCollection(name, indexes, fields, prefix) {
        if (prefix) {
            prefix += "_";
        }
        else {
            prefix = "";
        }
        let q = `CREATE TABLE "${name}" (`;
        q += []
            .concat(fields.map((f) => {
            let res = `"${f.name}" ${f.type}`;
            if (f.notNull) {
                res += " NOT NULL";
            }
            if (f.default !== undefined) {
                res += " DEFAULT " + f.default;
            }
            return res;
        }), indexes
            .filter((i) => i.key)
            .map((i) => `CONSTRAINT "${name}_${i.name}_pkey" PRIMARY KEY (` +
            i.key.map((k) => `"${k}"`).join(",") +
            ")"), indexes
            .filter((i) => i.unique)
            .map((i) => `CONSTRAINT "${name}_${i.name}_u" UNIQUE ("${i.name}")`), indexes
            .filter((i) => i.foreign)
            .map((i) => `CONSTRAINT "${name}_${i.name}_fkey" FOREIGN KEY ` +
            `("${i.name}") REFERENCES "${i.foreign.table}" ` +
            `(${i.foreign.field}) MATCH SIMPLE`))
            .join(",");
        q += ") WITH ( OIDS = FALSE )";
        try {
            return this._dbs.query(q);
        }
        catch (e) {
            this._log("Error in updateOne:", this._query, this._params, e);
            throw e;
        }
    }
    createUser() { }
    convertType(type) {
        switch (type.type) {
            case "int":
                return "integer";
            case "id":
                if (this._config.idsAsBigInt) {
                    return type.auto ? "bigserial" : "bigint";
                }
                else {
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
                }
                else {
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
        }
        catch (e) {
            this._log("Error in dbs.raw", query, params, e);
            throw e;
        }
    }
    _log(message, query, params, error) {
        if (this._config.logs === "errors") {
            if (this._config.logParams) {
                console.log(message, "\nQUERY:\n", query, "\nPARAMS:\n", params, "\nERROR:\n", error);
            }
            else {
                console.log(message, "\nQUERY:\n", query, "\nERROR:\n", error);
            }
        }
    }
}
exports.default = DBS;
//# sourceMappingURL=DBS.js.map