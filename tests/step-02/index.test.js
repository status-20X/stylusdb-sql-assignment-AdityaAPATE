const {readCSV} = require("../../src/csvStorage.js");
// const fs = require("fs"); // This line is removed

test("Read CSV file", async () => {
  const data = await readCSV("./sample.csv");
  expect(data.length).toBeGreaterThan(0);
  expect(data.length).toBe(3);
  expect(data[0].name).toBe("John");
  expect(data[0].age).toBe("30");
});
