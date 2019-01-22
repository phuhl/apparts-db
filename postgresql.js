"use strict";

const { Pool } = require('pg');
let pool;
let idsAsBigInt = false;

class DBS {

  constructor(dbs){
    this._dbs = dbs;
  }

  /* ID FUNCTIONS */
  newId(){
    return undefined;
  }

  fromId(id){
    return id;
  }

  toId(id){
    return id;
  }
  /* END ID FUNCTIONS */


  /* DBS FUNCTIONS */
  collection(col){
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
  createCollection(name, indexes, fields, prefix){
    if(prefix){
      prefix += '_';
    } else {
      prefix = '';
    }
    let q = `CREATE TABLE public."${name}" (`;
    q += [].concat(
      // fields
      fields.map(f => {
        let res = `"${f.name}" ${f.type}`;
        if(f.notNull){
          res += ' NOT NULL';
        }
        if(f.default !== undefined){
          res += ' DEFAULT ' + f.default;
        }
        return res;
      }),
      // constraints
      indexes
        .filter(i => i.key)
        .map(i => `CONSTRAINT "${name}_${i.name}_pkey" PRIMARY KEY (`
             + i.key.map(k => `"${k}"`).join(',') + ')'),
      indexes
        .filter(i => i.unique)
        .map(i => `CONSTRAINT "${name}_${i.name}_u" UNIQUE ("${i.name}")`),
      indexes
        .filter(i => i.foreign)
        .map(i => `CONSTRAINT "${name}_${i.name}_fkey" FOREIGN KEY `
             + `("${i.name}") REFERENCES public."${i.foreign.table}" `
             + `(${i.foreign.field}) MATCH SIMPLE`)
    ).join(',');
    // with
    q += ') WITH ( OIDS = FALSE )';
    return this._dbs.query(q);
  }

  createUser(user){
  }

  convertType(type){
    switch(type.type){
    case 'int':
      return 'integer';
    case 'id':
      if(idsAsBigInt){
        return type.name === '_id' ? 'bigserial' : 'bigint';
      } else {
        return type.name === '_id' ? 'serial' : 'integer';
      }
    case 'bool':
      return 'boolean';
    case 'float':
      return 'double precision';
    case '/':
    case 'string':
    case 'hex':
    case 'base64':
    case 'email':
    case 'password':
      return type.maxLength ? `varchar(${type.maxLength})` : 'text';
    case 'time':
      return 'bigint';
    case 'array_id':
      if(idsAsBigInt){
        return 'bigint[]';
      } else {
        return 'integer[]';
      }
    };
    throw new Error('ERROR: Type not found: ' + JSON.stringify(type));
  }

  shutdown(next){
    pool.end();
    next && next();
  }
  /* END DBS FUNCTIONS */
};

class Transaction{
  constructor(dbs, col){
    this._dbs = dbs;
    this._table = col;
    this._counter = 1;
  }

  find(params, limit){
    let q = `SELECT * FROM public."${this._table}" WHERE `;
    let newVals = [];
    q += this._buildWhere(params, newVals);
    if(limit){
      q += ` LIMIT $${this._counter++}`;
      newVals.push(limit);
    }
    this._query = q;
    this._params = newVals;
    return this;
  }

  _buildWhere(params, newVals){
    let keys = Object.keys(params);
    let vals = keys.map(key => params[key]);
    return keys.map((key, i) => {
      if(typeof vals[i] !== 'object'){
        newVals.push(vals[i]);
        return `"${key}" = $${this._counter++}`;
      } else {
        let op = vals[i].op;
        return this._decideOperator(key, op, vals[i].val, newVals);
      }
    }).join(' AND ');
  }

  _decideOperator(key, op, val, newVals){
    switch(op){
    case 'any':
      newVals.push(val);
      return `$${this._counter++} = ANY("${key}")`;
    case 'in':
      val.forEach(v => newVals.push(v));
      return `"${key}" IN (`
        + val.map((id, i) => `$${this._counter++}` ).join(',') + ')';
    case 'lte':
      newVals.push(val);
      return `"${key}" <= $${this._counter++}`;
    case 'lt':
      newVals.push(val);
      return `"${key}" < $${this._counter++}`;
    case 'gte':
      newVals.push(val);
      return `"${key}" >= $${this._counter++}`;
    case 'gt':
      newVals.push(val);
      return `"${key}" > $${this._counter++}`;
    case 'and':
      return val
        .map(v => this._decideOperator(key, v.op, v.val, newVals))
        .join(' AND ');
    default:
      throw new Error('ERROR, operator not implemented: ' + op);
    }
  }

  findById(id, limit){
    return this.find(id, limit);
  }

  findByIds(ids, limit){
    Object.keys(ids).forEach(key => {
      if(Array.isArray(ids[key])){
        ids[key] = { op: 'in', val: ids[key] };
      }
    });
    return this.find(ids, limit);
  }

  toArray(){
    this._result = this._dbs.query(this._query, this._params);

    return this._result
      .then(res => {
        return Promise.resolve(res.rows);
      });
  }

  insert(content, returning="_id", withID=false){
    let q = `INSERT INTO public."${this._table}" `;
    let keys = Object.keys(content[0]);
    if(!withID){
      keys = keys.filter(k => k !== '_id');
    }
    q += "(" + keys.map(key => `"${key}"`).join(',') + ")";
    q += ' VALUES ';
    q += content.map(
      (c, i) => '(' + keys.map(
        (k, j) =>`$${(i * keys.length) + (j + 1)}`).join(',') + ')'
    ).join(',');
    if(returning){
      q += ' RETURNING ' + returning;
    }
    return this._dbs.query(
      q, [].concat.apply(
        [], content.map(
          c => keys.map(
            k => Array.isArray(c[k]) ? '{' + c[k].join(',') + '}' : c[k]
          ))))
      .then(res => {
        return Promise.resolve(res.rows.map(r => r._id));
      })
      .catch(err => {
        if(err.code == 23505){
          return Promise.reject(
            { msg: 'ERROR, tried to insert, not unique',
              _code: 1 });
        } else {
          return Promise.reject(err);
        }
      });
  }

  updateOne(filter, c){
    let q = `UPDATE public."${this._table}" SET `;
    let counter = 1;
    let keys = Object.keys(c).filter(k => k !== '_id');
    q += '(' + keys.map(k => `"${k}"`).join(',') + ') = ';
    q += '(' + keys.map(k => `$${counter++}`).join(',') + ')';
    q += ' WHERE ';
    q += Object.keys(filter)
      .map((key, i) => `"${key}" = $${counter + i}` ).join(' AND ');
    let vals = keys.map(
      k => Array.isArray(c[k]) ? '{' + c[k].join(',') + '}' : c[k])
          .concat(Object.keys(filter).map(key => filter[key]));
    return this._dbs.query(q, vals);
  }

  remove(params){
    let q = `DELETE FROM public."${this._table}" WHERE `;
//    let keys = Object.keys(params);
    let newVals = [];
    q += this._buildWhere(params, newVals);
//    q += keys.map((key, i) => `"${key}" = $${i + 1}` ).join(' AND ');
//    let vals = keys.map(key => params[key]);
    return this._dbs.query(q, newVals);
  }

  drop(){
    let q = `DROP TABLE public."${this._table}"`;
    return this._dbs.query(q);
  }
}

module.exports.connect = function(c, next){
  pool = new Pool({
    host: c.host,
    port: c.port,
    user: c.user,
    password: c.pw,
    database: c.db,
    max: c.maxPoolSize || 10,
    connectionTimeoutMillis: c.connectionTimeoutMillis || 0,
    idleTimeoutMillis: c.idleTimeoutMillis || 10000
  });

  if(c.bigIntAsNumber){
    // Return Bigint and stuff as number, not as string
    const types = require('pg').types;
    types.setTypeParser(20, function(val) {
      return parseInt(val);
    });
  }

  if(c.idsAsBigInt){
    idsAsBigInt = true;
  }

  next(false, new DBS(pool));
};
