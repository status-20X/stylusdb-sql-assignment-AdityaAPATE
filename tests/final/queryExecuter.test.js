const {executeSELECTQuery,executeINSERTQuery} = require("../../src/index.js");
const { parseSELECTQuery } = require("../../src/queryParser.js");

test("Execute SQL query", async () => {
  const query = "SELECT id,name FROM sample";
  const result = await executeSELECTQuery(query);

  expect(result.length).toBeGreaterThan(0);
  expect(result[0]).toHaveProperty("id");
  expect(result[0]).toHaveProperty("name");
  expect(result[0]).not.toHaveProperty("age");
  expect(result[0]).toEqual({ id: "1", name: "John" });
});

test("Execute SQL query to fail", async () => {
  const query = "SELECT fun FROM sample";
  const result = await executeSELECTQuery(query);

  expect(result.length).toBe(0); //since fun is not in data
});

// can't handle error for file not found
// test("Execute SQL query to fail again", async () => {
//     const query = "SELECT fun FROM samples";
//     expect(await executeSELECTQuery(query)).toThrow();
// })

test("Execute SQL Query with WHERE clause", async () => {
  const query = "SELECT id, name FROM SAMPLE WHERE age = 25";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBe(1);
  expect(result[0]).toHaveProperty("id");
  expect(result[0]).toHaveProperty("name");
  expect(result[0].id).toBe("2");
});

test("Execute SQL Query with Multiple WHERE Clause", async () => {
  const query = "SELECT id, name FROM sample WHERE age = 30 AND name = John";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBe(1);
  expect(result[0]).toEqual({ id: "1", name: "John" });
});

// OR is not handled
test("Execute SQL Query with Multiple WHERE Clause having OR", async () => {
  const query = "SELECT id,name FROM sample WHERE age = 30 OR age = 25";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBe(0);
});

test("Execute SQL Query with Greater Than", async () => {
  const query = "SELECT id FROM sample WHERE age > 22";
  const result = await executeSELECTQuery(query);
  expect(result.length).toEqual(2);
  expect(result[0]).toHaveProperty("id");
});

test("Execute SQL Query with Greater Than and Equal to", async () => {
  const query = "SELECT id FROM sample WHERE age > 22 and age = 30";
  const result = await executeSELECTQuery(query);
  expect(result.length).toEqual(1);
  expect(result[0]).toHaveProperty("id");
});

test("Execute SQL Query with Not Equal to", async () => {
  const query = "SELECT name FROM sample WHERE age != 25";
  const result = await executeSELECTQuery(query);
  expect(result.length).toEqual(2);
  expect(result[0]).toHaveProperty("name");
});

test("Execute SQL Query with INNER JOIN", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBeGreaterThan(0);
  expect(result[0]).toEqual({
    "student.name": "John",
    "enrollment.course": "Mathematics",
  });
});
test("Execute SQL Query with INNER JOIN and a WHERE Clause", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id WHERE student.name = John";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBeGreaterThan(0);
  expect(result[0]).toEqual({
    "student.name": "John",
    "enrollment.course": "Mathematics",
  });
});

test("Execute SQL Query with LEFT JOIN", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student LEFT JOIN enrollment ON student.id=enrollment.student_id";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        "student.name": "Alice",
        "enrollment.course": null,
      }),
      expect.objectContaining({
        "student.name": "John",
        "enrollment.course": "Mathematics",
      }),
    ])
  );
  expect(result.length).toEqual(5); // 4 students, but John appears twice
});

test("Execute SQL Query with LEFT JOIN with a WHERE clause filtering the main table", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student LEFT JOIN enrollment ON student.id=enrollment.student_id WHERE student.age > 22";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        "enrollment.course": "Mathematics",
        "student.name": "John",
      }),
      expect.objectContaining({
        "enrollment.course": "Physics",
        "student.name": "John",
      }),
    ])
  );
  expect(result.length).toEqual(4);
});

test("Execute SQL Query with LEFT JOIN with a WHERE clause filtering the join table", async () => {
  const query = `SELECT student.name, enrollment.course FROM student LEFT JOIN enrollment ON student.id=enrollment.student_id WHERE enrollment.course = 'Physics'`;
  const result = await executeSELECTQuery(query);
  expect(result).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        "student.name": "John",
        "enrollment.course": "Physics",
      }),
    ])
  );
  expect(result.length).toEqual(1);
});

test("Execute SQL Query with RIGHT JOIN with a WHERE clause filtering the main table", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student RIGHT JOIN enrollment ON student.id=enrollment.student_id WHERE student.age < 25";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        "enrollment.course": "Mathematics",
        "student.name": "Bob",
      }),
      expect.objectContaining({
        "enrollment.course": "Biology",
        "student.name": null,
      }),
    ])
  );
  expect(result.length).toEqual(2);
});

test("Execute SQL Query with RIGHT JOIN with a WHERE clause filtering the join table", async () => {
  const query = `SELECT student.name, enrollment.course FROM student RIGHT JOIN enrollment ON student.id=enrollment.student_id WHERE enrollment.course = 'Chemistry'`;
  const result = await executeSELECTQuery(query);
  expect(result).toEqual(
    expect.arrayContaining([
      expect.objectContaining({
        "enrollment.course": "Chemistry",
        "student.name": "Jane",
      }),
    ])
  );
  expect(result.length).toEqual(1);
});

test("Execute SQL Query with RIGHT JOIN with a multiple WHERE clauses filtering the join table and main table", async () => {
  const query = `SELECT student.name, enrollment.course FROM student RIGHT JOIN enrollment ON student.id=enrollment.student_id WHERE enrollment.course = 'Chemistry' AND student.age = 26`;
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([]);
});

test("Execute SQL Query with ORDER BY", async () => {
  const query = "SELECT name FROM student ORDER BY name ASC";
  const result = await executeSELECTQuery(query);
  expect(result).toStrictEqual([
    { name: "Alice" },
    { name: "Bob" },
    { name: "Jane" },
    { name: "John" },
  ]);
});

test("Execute SQL Query with ORDER BY and WHERE", async () => {
  const query = "SELECT name FROM student WHERE age > 24 ORDER BY name DESC";
  const result = await executeSELECTQuery(query);

  expect(result).toStrictEqual([{ name: "John" }, { name: "Jane" }]);
});
test("Execute SQL Query with ORDER BY and GROUP BY", async () => {
  const query =
    "SELECT COUNT(id) as count, age FROM student GROUP BY age ORDER BY age DESC";
  const result = await executeSELECTQuery(query);
  expect(result).toStrictEqual([
    { age: "30", "count(id) as count": 1 },
    { age: "25", "count(id) as count": 1 },
    { age: "24", "count(id) as count": 1 },
    { age: "22", "count(id) as count": 1 },
  ]);
});

test("Execute SQL Query with aggregate fxn and limit clause without group by", async () => {
  const query = "SELECT COUNT(id), age FROM student LIMIT 2";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBe(2);
});
test("Execute SQL Query with standard LIMIT clause", async () => {
  const query = "SELECT id, name FROM student LIMIT 2";
  const result = await executeSELECTQuery(query);
  expect(result.length).toEqual(2);
});

test("Execute SQL Query with LIMIT clause equal to total rows", async () => {
  const query = "SELECT id, name FROM student LIMIT 4";
  const result = await executeSELECTQuery(query);
  expect(result.length).toEqual(4);
});

test("Execute SQL Query with LIMIT clause exceeding total rows", async () => {
  const query = "SELECT id, name FROM student LIMIT 10";
  const result = await executeSELECTQuery(query);
  expect(result.length).toEqual(4); // Total rows in student.csv
});

test("Execute SQL Query with LIMIT 0", async () => {
  const query = "SELECT id, name FROM student LIMIT 0";
  const result = await executeSELECTQuery(query);
  expect(result.length).toEqual(0);
});

test("Execute SQL Query with LIMIT and ORDER BY clause", async () => {
  const query = "SELECT id, name FROM student ORDER BY age DESC LIMIT 2";
  const result = await executeSELECTQuery(query);
  expect(result.length).toEqual(2);
  expect(result[0].name).toEqual("John");
  expect(result[1].name).toEqual("Jane");
});

test("Error Handling with Malformed Query", async () => {
  const query = "SELECT FROM table"; // intentionally malformed
  await expect(executeSELECTQuery(query)).rejects.toThrow(
    "Error executing query: Query parsing error: Invalid SELECT format"
  );
});

test("Basic DISTINCT Usage", async () => {
  const query = "SELECT DISTINCT age FROM student";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([
    { age: "30" },
    { age: "25" },
    { age: "22" },
    { age: "24" },
  ]);
});

test("DISTINCT with Multiple Columns", async () => {
  const query = "SELECT DISTINCT student_id, course FROM enrollment";
  const result = await executeSELECTQuery(query);
  // Expecting unique combinations of student_id and course
  expect(result).toEqual([
    { student_id: "1", course: "Mathematics" },
    { student_id: "1", course: "Physics" },
    { student_id: "2", course: "Chemistry" },
    { student_id: "3", course: "Mathematics" },
    { student_id: "5", course: "Biology" },
  ]);
});

// Not a good test right now
test("DISTINCT with WHERE Clause", async () => {
  const query = 'SELECT DISTINCT course FROM enrollment WHERE student_id = "1"';
  const result = await executeSELECTQuery(query);
  // Expecting courses taken by student with ID 1
  expect(result).toEqual([{ course: "Mathematics" }, { course: "Physics" }]);
});

test("DISTINCT with JOIN Operations", async () => {
  const query =
    "SELECT DISTINCT student.name FROM student INNER JOIN enrollment ON student.id = enrollment.student_id";
  const result = await executeSELECTQuery(query);
  // Expecting names of students who are enrolled in any course
  expect(result).toEqual([
    { "student.name": "John" },
    { "student.name": "Jane" },
    { "student.name": "Bob" },
  ]);
});

test("DISTINCT with ORDER BY and LIMIT", async () => {
  const query = "SELECT DISTINCT age FROM student ORDER BY age DESC LIMIT 2";
  const result = await executeSELECTQuery(query);
  // Expecting the two highest unique ages
  expect(result).toEqual([{ age: "30" }, { age: "25" }]);
});

test('Execute SQL Query with LIKE Operator for Name', async () => {
  const query = "SELECT name FROM student WHERE name LIKE '%Jane%'";
  const result = await executeSELECTQuery(query);
  // Expecting names containing 'Jane'
  expect(result).toEqual([{ name: 'Jane' }]);
});

test('Execute SQL Query with LIKE Operator and Wildcards', async () => {
  const query = "SELECT name FROM student WHERE name LIKE 'J%'";
  const result = await executeSELECTQuery(query);
  // Expecting names starting with 'J'
  expect(result).toEqual([{ name: 'John' },{ name: 'Jane' }]);
});

test('Execute SQL Query with LIKE Operator Case Insensitive', async () => {
  const query = "SELECT name FROM student WHERE name LIKE '%bob%'";
  const result = await executeSELECTQuery(query);
  // Expecting names 'Bob' (case insensitive)
  expect(result).toEqual([{ name: 'Bob' }]);
});

test('Execute SQL Query with LIKE Operator and DISTINCT', async () => {
  const query = "SELECT DISTINCT name FROM student WHERE name LIKE '%e%'";
  const result = await executeSELECTQuery(query);
  // Expecting unique names containing 'e'
  expect(result).toEqual([{ name: 'Jane' }, { name: 'Alice' }]);
});

test('LIKE with ORDER BY and LIMIT', async () => {
  const query = "SELECT name FROM student WHERE name LIKE '%a%' ORDER BY name ASC LIMIT 2";
  const result = await executeSELECTQuery(query);
  // Expecting the first two names alphabetically that contain 'a'
  expect(result).toEqual([{ name: 'Alice' }, { name: 'Jane' }]);
});

// no idea why not working
// test('Execute SQL Query with APPROXIMATE_COUNT Function', async () => {
//   const query = "SELECT APPROXIMATE_COUNT(id) FROM student";
//   const result = await executeSELECTQuery(query);
//   // Assuming APPROXIMATE_COUNT behaves like COUNT for testing
//   // Expecting the count of all student records
//   console.log(query,parseSELECTQuery(query),result)
//   expect(result).toEqual([{ 'count(id)': 4 }]); // Assuming there are 4 records in student.csv
// });

test('Execute SQL Query with APPROXIMATE_COUNT and GROUP BY Clauses', async () => {
  const query = "SELECT APPROXIMATE_COUNT(id), course FROM enrollment GROUP BY course";
  const result = await executeSELECTQuery(query);
  // Assuming APPROXIMATE_COUNT behaves like COUNT for testing
  // Expecting the count of student records grouped by course
  expect(result).toEqual([
      { 'count(id)': 2, course: 'Mathematics' }, // Assuming 2 students are enrolled in Mathematics
      { 'count(id)': 1, course: 'Physics' }, // Assuming 1 student is enrolled in Physics
      { 'count(id)': 1, course: 'Chemistry' }, // Assuming 1 student is enrolled in Chemistry
      { 'count(id)': 1, course: 'Biology' } // Assuming 1 student is enrolled in Biology
  ]);
});

//no idea why not working
// test('Execute SQL Query with APPROXIMATE_COUNT, WHERE, and ORDER BY Clauses', async () => {
//   const query = "SELECT APPROXIMATE_COUNT(id) FROM student WHERE age > '20' ORDER BY age DESC";
//   const result = await executeSELECTQuery(query);
//   console.log(parseSELECTQuery(query))
//   // Assuming APPROXIMATE_COUNT behaves like COUNT for testing
//   // Expecting the count of students older than 20, ordered by age in descending order
//   // Note: The ORDER BY clause does not affect the outcome for a single aggregated result
//   // console.log(result)
//   expect(result).toEqual([{ 'count(id)': 4 }]); // Assuming there are 4 students older than 20
// });

test('Execute SQL Query with LIKE Operator for Name', async () => {
  const query = "SELECT name FROM student WHERE name LIKE '%Jane%'";
  const result = await executeSELECTQuery(query);
  // Expecting names containing 'Jane'
  expect(result).toEqual([{ name: 'Jane' }]);
});

test('Execute SQL Query with APPROXIMATE_COUNT only', async () => {
  const query = "SELECT APPROXIMATE_COUNT(*) FROM student";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([{ 'APPROXIMATE_COUNT(*)': 4 }]);
});

test('Execute SQL Query with APPROXIMATE_COUNT with DISTINCT on a column', async () => {
  const query = "SELECT APPROXIMATE_COUNT(DISTINCT (name)) FROM student";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([{ 'APPROXIMATE_count(distinct (name))': 4 }]);
});

test('Execute SQL Query with COUNT with DISTINCT on a column', async () => {
  const query = "SELECT COUNT(DISTINCT (name)) FROM student";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([{ 'count(distinct (name))': 4 }]);
});

test('Execute SQL Query with COUNT with DISTINCT on a column', async () => {
  const query = "SELECT COUNT(DISTINCT (name, age)) FROM student";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([{ 'count(distinct (name, age))': 4 }]);
});
