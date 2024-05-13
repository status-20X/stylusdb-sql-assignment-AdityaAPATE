const {readCSV, writeCSV} = require("../../src/csvStorage.js");
const fs = require("fs");

test("Read CSV file", async () => {
  const data = await readCSV("./sample.csv");
  expect(data.length).toBeGreaterThan(0);
  expect(data.length).toBe(3);
  expect(data[0].name).toBe("John");
  expect(data[0].age).toBe("30");
});

describe('writeCSV Function', () => {
  const testFilename = 'test_output.csv';

  afterAll(() => {
      // Cleanup: Delete the test file after the test
      if (fs.existsSync(testFilename)) {
          fs.unlinkSync(testFilename);
      }
  });

  test('Should create a CSV file with correct contents', async () => {
      const testData = [
          { column1: 'data1', column2: 'data2' },
          { column1: 'data3', column2: 'data4' }
      ];

      await writeCSV(testFilename, testData);

      // Check if file exists
      expect(fs.existsSync(testFilename)).toBe(true);

      // Read the file and verify its contents
      const fileContents = fs.readFileSync(testFilename, 'utf8');
      const expectedContents = `"column1","column2"\n"data1","data2"\n"data3","data4"`;
      expect(fileContents).toBe(expectedContents);
  });
});
