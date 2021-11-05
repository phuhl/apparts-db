const { Client } = require("pg");
const connect = require("../").default;
const dbConfig = require("@apparts/config").get("db-test-config");

const createOrDropDatabase = async (action, db_config, dbName) => {
  const config = { ...db_config };
  config.database = "postgres";

  const client = new Client(config);
  //disconnect client when all queries are finished
  //  client.on('drain', client.end.bind(client));
  client.on("error", async (err) => {
    console.log("COULD NOT " + action + " DATABASE " + dbName + ": " + err);
    await client.end.bind(client);
    throw "COULD NOT " + action + " DATABASE " + dbName + ": " + err;
  });
  await client.connect();

  const escapedDbName = dbName.replace(/"/g, '""');
  const sql = action + ' DATABASE "' + escapedDbName + '"';
  try {
    await client.query(sql);
  } catch (e) {
    await client.end();
    throw e;
  }
  await client.end();
};

module.exports = ({ testName }) => {
  const dbName = dbConfig.postgresql.db + "_" + testName;

  beforeAll(async () => {
    try {
      await createOrDropDatabase("DROP", dbConfig.postgresql, dbName);
    } catch (e) {
      if (e.code !== "3D000") {
        console.log(e);
      }
    }
    try {
      await createOrDropDatabase("CREATE", dbConfig.postgresql, dbName);
    } catch (e) {
      console.log("ERROR", e);
      throw e;
    }
  }, 60000);

  afterAll(async () => {
    // avoid jest open handle error
    // https://github.com/visionmedia/supertest/issues/520#issuecomment-469044925
    await new Promise((resolve) => setTimeout(() => resolve(), 500));
  }, 60000);

  return {
    dbName,
    dbConfig: {
      ...dbConfig,
      postgresql: {
        ...dbConfig.postgresql,
        db: dbName,
      },
    },
    setupDbs: async (config) => {
      return await new Promise((res) => {
        const next = (e, c) => {
          if (e) {
            throw e;
          }
          res(c);
        };
        connect(
          {
            ...dbConfig,
            postgresql: {
              ...dbConfig.postgresql,
              db: dbName,
              ...config,
            },
          },
          next
        );
      });
    },

    teardownDbs: async (dbs) => {
      await new Promise((res) => {
        dbs.shutdown(res);
      });
    },
  };
};
