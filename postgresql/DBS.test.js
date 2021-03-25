const { setupDbs, teardownDbs } = require("../tests/pg")({
  testName: "dbstest",
});

describe("Prints errors with params", () => {
  let dbs;
  beforeAll(async () => {
    dbs = await setupDbs({
      logs: "errors",
      logParams: true,
    });
  });
  afterAll(async () => {
    await teardownDbs(dbs);
  });

  test("Should log raw query failure", async () => {
    const logMock = jest.spyOn(console, "log").mockImplementation(() => {});
    let error;
    try {
      await dbs.raw(`SELECT FROM "testTable"`, [1, "test"]);
      expect(true).toBe(false);
    } catch (e) {
      error = e;
    }

    expect(logMock.mock.calls).toEqual([
      [
        "Error in dbs.raw",
        "\nQUERY:\n",
        `SELECT FROM "testTable"`,
        "\nPARAMS:\n",
        [1, "test"],
        "\nERROR:\n",
        error,
      ],
    ]);
    logMock.mockRestore();
  });
});

describe("Prints errors without params", () => {
  let dbs;
  beforeAll(async () => {
    dbs = await setupDbs({ logs: "errors" });
  });
  afterAll(async () => {
    await teardownDbs(dbs);
  });

  test("Should log raw query failure", async () => {
    const logMock = jest.spyOn(console, "log").mockImplementation(() => {});
    let error;
    try {
      await dbs.raw(`SELECT FROM "testTable"`, [1, "test"]);
      expect(true).toBe(false);
    } catch (e) {
      error = e;
    }

    expect(logMock.mock.calls).toEqual([
      [
        "Error in dbs.raw",
        "\nQUERY:\n",
        `SELECT FROM "testTable"`,
        "\nERROR:\n",
        error,
      ],
    ]);
    logMock.mockRestore();
  });
});

describe("Postgresql DBS", () => {
  let dbs;
  beforeAll(async () => {
    dbs = await setupDbs();
  });
  afterAll(async () => {
    await teardownDbs(dbs);
  });

  test("Should run raw query", async () => {
    await dbs.raw(`
CREATE TABLE "testTable" (
       id SERIAL PRIMARY KEY,
       number INT NOT NULL
)`);
    await dbs.raw(`INSERT INTO "testTable" ( number ) VALUES ($1), ($2)`, [
      1,
      7,
    ]);
    const { rows } = await dbs.raw(`SELECT number FROM "testTable"`);
    expect(rows).toEqual([{ number: 1 }, { number: 7 }]);
  });
});
