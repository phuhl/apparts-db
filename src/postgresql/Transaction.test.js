const { setupDbs, teardownDbs } = require("../tests/pg")({
  testName: "transactiontest",
});
const DBS = require("./DBS").default;
const Transaction = require("./Transaction").default;

describe("Log on error behavior", () => {
  let logMock, e, query, dbs;
  beforeEach(() => {
    logMock = jest.spyOn(console, "log").mockImplementation(() => {});
    e = new Error("test");
    query = jest.fn().mockImplementation(async () => {
      throw e;
    });
    dbs = new DBS({}, { logs: "errors", logParams: true });
  });
  afterEach(() => {
    logMock.mockRestore();
  });

  test("Should log on updateOne", async () => {
    const t = dbs.collection("testTable");
    t._dbs = { query };

    await expect(t.updateOne({ number: 100 }, { number: 1000 })).rejects.toBe(
      e
    );
    expect(logMock.mock.calls).toEqual([
      [
        "Error in updateOne:",
        "\nQUERY:\n",
        `UPDATE "testTable" SET "number" = $1 WHERE "number" = $2`,
        "\nPARAMS:\n",
        [1000, 100],
        "\nERROR:\n",
        e,
      ],
    ]);
  });
  test("Should log on remove", async () => {
    const t = dbs.collection("testTable");
    t._dbs = { query };

    await expect(t.remove({ number: 100 })).rejects.toBe(e);
    expect(logMock.mock.calls).toEqual([
      [
        "Error in remove:",
        "\nQUERY:\n",
        `DELETE FROM "testTable" WHERE "number" = $1`,
        "\nPARAMS:\n",
        [100],
        "\nERROR:\n",
        e,
      ],
    ]);
  });
  test("Should log on drop", async () => {
    const t = dbs.collection("testTable");
    t._dbs = { query };

    await expect(t.drop()).rejects.toBe(e);
    expect(logMock.mock.calls).toEqual([
      [
        "Error in drop:",
        "\nQUERY:\n",
        `DROP TABLE "testTable"`,
        "\nPARAMS:\n",
        null,
        "\nERROR:\n",
        e,
      ],
    ]);
  });

  test("Should log on insert", async () => {
    const t = dbs.collection("testTable");
    t._dbs = { query };

    await expect(t.insert([{ a: 2 }])).rejects.toBe(e);
    expect(logMock.mock.calls).toEqual([
      [
        "Error in insert:",
        "\nQUERY:\n",
        `INSERT INTO "testTable" ("a") VALUES ($1) RETURNING "id"`,
        "\nPARAMS:\n",
        [2],
        "\nERROR:\n",
        e,
      ],
    ]);
  });

  test("Should log on find", async () => {
    const t = dbs.collection("testTable");
    t._dbs = { query };

    await expect(t.find({ a: 2 }).toArray()).rejects.toBe(e);
    expect(logMock.mock.calls).toEqual([
      [
        "Error in toArray:",
        "\nQUERY:\n",
        `SELECT * FROM "testTable" WHERE "a" = $1`,
        "\nPARAMS:\n",
        [2],
        "\nERROR:\n",
        e,
      ],
    ]);
  });
  test("Should log on findById", async () => {
    const t = dbs.collection("testTable");
    t._dbs = { query };

    await expect(t.findById({ a: 2 }).toArray()).rejects.toBe(e);
    expect(logMock.mock.calls).toEqual([
      [
        "Error in toArray:",
        "\nQUERY:\n",
        `SELECT * FROM "testTable" WHERE "a" = $1`,
        "\nPARAMS:\n",
        [2],
        "\nERROR:\n",
        e,
      ],
    ]);
  });
  test("Should log on findByIds", async () => {
    const t = dbs.collection("testTable");
    t._dbs = { query };

    await expect(t.findByIds({ id: [2] }).toArray()).rejects.toBe(e);
    expect(logMock.mock.calls).toEqual([
      [
        "Error in toArray:",
        "\nQUERY:\n",
        `SELECT * FROM "testTable" WHERE "id" IN ($1)`,
        "\nPARAMS:\n",
        [2],
        "\nERROR:\n",
        e,
      ],
    ]);
  });
});

describe("Postgresql Transaction", () => {
  let dbs;
  beforeAll(async () => {
    dbs = await setupDbs({ logs: "errors" });
  });
  afterAll(async () => {
    await teardownDbs(dbs);
  });

  test("Should insert", async () => {
    await dbs.raw(`
CREATE TABLE "testTable" (
       id SERIAL PRIMARY KEY,
       number INT NOT NULL
)`);
    await dbs.raw(`
CREATE TABLE "testTable2" (
       id SERIAL PRIMARY KEY,
       "testTableId" INT NOT NULL,
       FOREIGN KEY ("testTableId") REFERENCES "testTable"(id)
)`);

    await dbs.raw(`
CREATE TABLE "testTable3" (
       id SERIAL PRIMARY KEY,
       "object1" json NOT NULL
)`);

    await expect(dbs.collection("testTable").insert([])).resolves.toMatchObject(
      []
    );

    await expect(
      dbs.collection("testTable").insert([{ number: 100 }])
    ).resolves.toMatchObject([{ id: 1 }]);

    await expect(
      dbs.collection("testTable").insert([{ number: 101 }, { number: 102 }])
    ).resolves.toMatchObject([{ id: 2 }, { id: 3 }]);

    await expect(
      dbs.collection("testTable").insert([{ number: 100, id: 1 }])
    ).rejects.toMatchObject({
      msg: "ERROR, tried to insert, not unique",
      _code: 1,
    });

    await expect(
      dbs.collection("testTable2").insert([{ testTableId: 10000 }])
    ).rejects.toMatchObject({
      msg: "ERROR, tried to insert, constraints not met",
      _code: 3,
    });

    await expect(
      dbs.collection("testTable2").find({}).toArray()
    ).resolves.toStrictEqual([]);

    await expect(
      dbs
        .collection("testTable3")
        .insert([{ object1: { object2: { tokens: "abc" } } }])
    ).resolves.toStrictEqual([{ id: 1 }]);

    await expect(
      dbs.collection("testTable3").insert([{ object1: { tokens: "abc" } }])
    ).resolves.toStrictEqual([{ id: 2 }]);
  });

  test("Should findById/find", async () => {
    await expect(
      dbs.collection("testTable").findById({ id: 1 }).toArray()
    ).resolves.toMatchObject([{ id: 1, number: 100 }]);

    await expect(
      dbs.collection("testTable").findById({ id: 100 }).toArray()
    ).resolves.toMatchObject([]);

    await expect(
      dbs.collection("testTable").findById({ id: 1, number: 100 }).toArray()
    ).resolves.toMatchObject([{ id: 1, number: 100 }]);

    await expect(
      dbs.collection("testTable").findById({ id: 1, number: 101 }).toArray()
    ).resolves.toMatchObject([]);

    await expect(
      dbs.collection("testTable").findById({}).toArray()
    ).resolves.toMatchObject([
      { id: 1, number: 100 },
      { id: 2, number: 101 },
      { id: 3, number: 102 },
    ]);

    await expect(
      dbs.collection("testTable").findById({}, 1).toArray()
    ).resolves.toMatchObject([{ id: 1, number: 100 }]);

    await expect(
      dbs.collection("testTable").findById({}, 1, 1).toArray()
    ).resolves.toMatchObject([{ id: 2, number: 101 }]);

    await expect(
      dbs
        .collection("testTable")
        .findById({ number: { op: "gt", val: 100 } })
        .toArray()
    ).resolves.toMatchObject([
      { id: 2, number: 101 },
      { id: 3, number: 102 },
    ]);

    await expect(
      dbs
        .collection("testTable")
        .findById({ number: { op: "lt", val: 102 } })
        .toArray()
    ).resolves.toMatchObject([
      { id: 1, number: 100 },
      { id: 2, number: 101 },
    ]);

    await expect(
      dbs
        .collection("testTable")
        .findById({ number: { op: "gte", val: 101 } })
        .toArray()
    ).resolves.toMatchObject([
      { id: 2, number: 101 },
      { id: 3, number: 102 },
    ]);

    await expect(
      dbs
        .collection("testTable")
        .findById({ number: { op: "lte", val: 101 } })
        .toArray()
    ).resolves.toMatchObject([
      { id: 1, number: 100 },
      { id: 2, number: 101 },
    ]);

    await expect(
      dbs
        .collection("testTable3")
        .find({
          object1: {
            op: "of",
            val: {
              path: ["object2", "tokens"],
              value: "abc",
            },
          },
        })
        .toArray()
    ).resolves.toMatchObject([
      { id: 1, object1: { object2: { tokens: "abc" } } },
    ]);

    await expect(
      dbs
        .collection("testTable3")
        .find({
          object1: {
            op: "of",
            val: {
              path: ["tokens"],
              value: "abc",
            },
          },
        })
        .toArray()
    ).resolves.toMatchObject([{ id: 2, object1: { tokens: "abc" } }]);

    expect(async () => {
      await dbs
        .collection("testTable3")
        .find({
          object1: {
            op: "of",
            val: {
              path: [],
              value: "abc",
            },
          },
        })
        .toArray();
    }).rejects.toThrow(
      "ERROR, operator 'of' requires at least one path element. You submitted []."
    );
  });

  test("Should findByIds", async () => {
    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1] })
        .toArray()
    ).resolves.toMatchObject([{ id: 1, number: 100 }]);

    await expect(
      dbs.collection("testTable").findByIds({ id: 1 }).toArray()
    ).resolves.toMatchObject([{ id: 1, number: 100 }]);

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [100] })
        .toArray()
    ).resolves.toMatchObject([]);

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1], number: [100] })
        .toArray()
    ).resolves.toMatchObject([{ id: 1, number: 100 }]);

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1], number: [101] })
        .toArray()
    ).resolves.toMatchObject([]);

    await expect(
      dbs.collection("testTable").findByIds({}).toArray()
    ).resolves.toMatchObject([
      { id: 1, number: 100 },
      { id: 2, number: 101 },
      { id: 3, number: 102 },
    ]);

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1, 2, 3] })
        .toArray()
    ).resolves.toMatchObject([
      { id: 1, number: 100 },
      { id: 2, number: 101 },
      { id: 3, number: 102 },
    ]);

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1, 2, 3] }, 1)
        .toArray()
    ).resolves.toMatchObject([{ id: 1, number: 100 }]);

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1, 2, 3] }, 1, 1)
        .toArray()
    ).resolves.toMatchObject([{ id: 2, number: 101 }]);

    await expect(
      dbs.collection("testTable").findByIds({ id: [] }).toArray()
    ).resolves.toMatchObject([]);
  });

  test("Should find in order", async () => {
    await expect(
      dbs.collection("testTable").insert([{ number: 100 }])
    ).resolves.toMatchObject([{ id: 4 }]);

    await expect(
      dbs
        .collection("testTable")
        .findById({}, null, null, [{ key: "id", dir: "DESC" }])
        .toArray()
    ).resolves.toMatchObject([
      { id: 4, number: 100 },
      { id: 3, number: 102 },
      { id: 2, number: 101 },
      { id: 1, number: 100 },
    ]);

    await expect(
      dbs
        .collection("testTable")
        .findById({}, null, null, [
          { key: "number", dir: "ASC" },
          { key: "id", dir: "DESC" },
        ])
        .toArray()
    ).resolves.toMatchObject([
      { id: 4, number: 100 },
      { id: 1, number: 100 },
      { id: 2, number: 101 },
      { id: 3, number: 102 },
    ]);
  });

  test("Should update/updateOne", async () => {
    await expect(
      dbs.collection("testTable").updateOne({ number: 100 }, { number: 1000 })
    ).resolves.toMatchObject({ rowCount: 2 });

    await expect(
      dbs
        .collection("testTable")
        .updateOne({ number: 101, id: 2 }, { number: 3000, id: 5 })
    ).resolves.toMatchObject({ rowCount: 1 });

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1, 2, 3, 4, 5] })
        .toArray()
    ).resolves.toMatchObject([
      { id: 1, number: 1000 },
      { id: 3, number: 102 },
      { id: 4, number: 1000 },
      { id: 5, number: 3000 },
    ]);

    await expect(
      dbs.collection("testTable").updateOne({}, { number: 2000 })
    ).resolves.toMatchObject({ rowCount: 4 });

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1, 5, 3] })
        .toArray()
    ).resolves.toMatchObject([
      { id: 1, number: 2000 },
      { id: 3, number: 2000 },
      { id: 5, number: 2000 },
    ]);

    await expect(
      dbs.collection("testTable").updateOne({}, { id: 1 })
    ).rejects.toMatchObject({
      msg: "ERROR, tried to update, not unique",
      _code: 1,
    });
  });

  test("Should remove", async () => {
    await expect(
      dbs.collection("testTable").remove({ number: 2000, id: 5 })
    ).resolves.toMatchObject({ rowCount: 1 });

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1, 2, 3, 5] })
        .toArray()
    ).resolves.toMatchObject([
      { id: 1, number: 2000 },
      { id: 3, number: 2000 },
    ]);

    await expect(
      dbs.collection("testTable2").insert([{ testTableId: 3 }])
    ).resolves.toMatchObject([{ id: 2 }]);

    await expect(
      dbs.collection("testTable").remove({ number: 2000 })
    ).rejects.toMatchObject({
      msg: "ERROR, tried to remove item that is still a reference",
      _code: 2,
    });

    await expect(
      dbs.collection("testTable2").remove({})
    ).resolves.toMatchObject({ rowCount: 1 });

    await expect(
      dbs.collection("testTable").remove({ number: 2000 })
    ).resolves.toMatchObject({ rowCount: 3 });

    await expect(
      dbs
        .collection("testTable")
        .findByIds({ id: [1, 2, 3, 4] })
        .toArray()
    ).resolves.toMatchObject([]);

    await expect(
      dbs.collection("testTable").remove({ number: 2000 })
    ).resolves.toMatchObject({ rowCount: 0 });
  });

  test("Should drop", async () => {
    await expect(dbs.collection("testTable2").drop()).resolves.toMatchObject(
      {}
    );
    const logMock = jest.spyOn(console, "log").mockImplementation(() => {});
    await expect(
      dbs.collection("testTable2").findByIds({}).toArray()
    ).rejects.toMatchObject({ code: "42P01" });
    logMock.mockRestore();
  });
});

describe("Postgresql Transaction JSON Data types", () => {
  let dbs;
  beforeAll(async () => {
    dbs = await setupDbs({ logs: "errors", arrayAsJSON: true });
  });
  afterAll(async () => {
    await teardownDbs(dbs);
  });

  test("Should insert", async () => {
    await dbs.raw(`
CREATE TABLE "testJson" (
       id SERIAL PRIMARY KEY,
       "jsonField" json,
       "jsonArray" json
)`);

    await expect(
      dbs
        .collection("testJson")
        .insert([{ jsonField: { a: 1 }, jsonArray: [1, 2, 3] }])
    ).resolves.toMatchObject([{ id: 1 }]);
  });
  test("Should update/updateOne", async () => {
    await expect(
      dbs.collection("testJson").updateOne({ id: 1 }, { jsonArray: [1, 2, 4] })
    ).resolves.toMatchObject({ rowCount: 1 });
  });

  test("Should find", async () => {
    await expect(
      dbs.collection("testJson").find({}).toArray()
    ).resolves.toMatchObject([
      { id: 1, jsonArray: [1, 2, 4], jsonField: { a: 1 } },
    ]);
  });
});

describe("Postgresql Transaction Non-JSON Data types", () => {
  let dbs;
  beforeAll(async () => {
    dbs = await setupDbs({ logs: "errors" });
  });
  afterAll(async () => {
    await teardownDbs(dbs);
  });

  test("Should insert", async () => {
    await dbs.raw(`
CREATE TABLE "testNonJson" (
       id SERIAL PRIMARY KEY,
       "jsonField" json,
       "nonJsonArray" integer[]
)`);

    await expect(
      dbs
        .collection("testNonJson")
        .insert([{ jsonField: { a: 1 }, nonJsonArray: [1, 2, 3] }])
    ).resolves.toMatchObject([{ id: 1 }]);
  });

  test("Should update/updateOne", async () => {
    await expect(
      dbs
        .collection("testNonJson")
        .updateOne({ id: 1 }, { nonJsonArray: [1, 2, 4] })
    ).resolves.toMatchObject({ rowCount: 1 });
  });

  test("Should find", async () => {
    await expect(
      dbs.collection("testNonJson").find({}).toArray()
    ).resolves.toMatchObject([
      { id: 1, nonJsonArray: [1, 2, 4], jsonField: { a: 1 } },
    ]);
  });
});
