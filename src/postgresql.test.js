const connect = require("./").default;
const { dbConfig } = require("./tests/pg")({
  testName: "postgresqltest",
});

describe("Postgresql connect", () => {
  test("Should connect and disconnect", async () => {
    const p = new Promise((res) => {
      const next = (...ps) => {
        res(ps);
      };
      connect(dbConfig, next);
    });

    const params = await p;
    expect(params[0]).toBe(false);
    const pShutdown = new Promise((res) => {
      params[1].shutdown(res);
    });
    await expect(pShutdown).resolves.not.toThrow();
  });
});
