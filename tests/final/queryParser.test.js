const {
  parseSELECTQuery,
  parseINSERTQuery,
  parseDELETEQuery,
} = require("../../src/queryParser.js");

// id,name, will return fields: [id,name,""]; this is a problem
//it should throw error
test("Parse SQL Query", () => {
  const query = "SELECT id,name FROM sample";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["id", "name"],
    table: "sample",
    whereClauses: [],
    joinType: null,
    joinCondition: null,
    joinTable: null,
    groupByFields: null,
    hasAggregateWithoutGroupBy: false,
    orderByFields: null,
    limit: null,
    isDistinct: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query throw error", () => {
  const query = "HI";

  expect(() => {
    parseSELECTQuery(query);
  }).toThrow();
});

test("Parse SQL Query with Multiple WHERE Clauses", () => {
  const query = "SELECT id, name FROM sample WHERE age = 30 AND name = John";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["id", "name"],
    table: "sample",
    whereClauses: [
      {
        field: "age",
        operator: "=",
        value: "30",
      },
      {
        field: "name",
        operator: "=",
        value: "John",
      },
    ],
    joinType: null,
    joinCondition: null,
    joinTable: null,
    groupByFields: null,
    hasAggregateWithoutGroupBy: false,
    orderByFields: null,
    limit: null,
    isDistinct: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with INNER JOIN", () => {
  const query =
    "SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id";
  const result = parseSELECTQuery(query);
  expect(result).toEqual({
    fields: ["student.name", "enrollment.course"],
    table: "student",
    whereClauses: [],
    joinType: "inner",
    joinTable: "enrollment",
    joinCondition: { left: "student.id", right: "enrollment.student_id" },
    groupByFields: null,
    hasAggregateWithoutGroupBy: false,
    orderByFields: null,
    limit: null,
    isDistinct: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with INNER JOIN and WHERE Clause", () => {
  const query =
    "SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id WHERE student.name = John";
  const result = parseSELECTQuery(query);
  expect(result).toEqual({
    fields: ["student.name", "enrollment.course"],
    table: "student",
    whereClauses: [
      {
        field: "student.name",
        operator: "=",
        value: "John",
      },
    ],
    joinType: "inner",
    joinTable: "enrollment",
    joinCondition: { left: "student.id", right: "enrollment.student_id" },
    groupByFields: null,
    hasAggregateWithoutGroupBy: false,
    orderByFields: null,
    limit: null,
    isDistinct: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with GroupBy clause", () => {
  const query = "SELECT id from student group by id, enrollment.student_id";
  const result = parseSELECTQuery(query);
  expect(result).toEqual(
    expect.objectContaining({
      groupByFields: expect.arrayContaining(["id", "enrollment.student_id"]),
    })
  );
});

test("Parse SQL Query with ORDER BY", () => {
  const query = "SELECT name FROM student ORDER BY name ASC";
  const parsed = parseSELECTQuery(query);
  expect(parsed.orderByFields).toEqual([{ fieldName: "name", order: "ASC" }]);
});

test("Parse SQL Query with ORDER BY and WHERE", () => {
  const query = "SELECT name FROM student WHERE age > 20 ORDER BY name DESC";
  const parsed = parseSELECTQuery(query);
  expect(parsed.orderByFields).toEqual([{ fieldName: "name", order: "DESC" }]);
  expect(parsed.whereClauses.length).toBeGreaterThan(0);
});

test("Parse SQL Query with ORDER BY and GROUP BY", () => {
  const query =
    "SELECT COUNT(id), age FROM student GROUP BY age ORDER BY age DESC";
  const parsed = parseSELECTQuery(query);
  expect(parsed.orderByFields).toEqual([{ fieldName: "age", order: "DESC" }]);
  expect(parsed.groupByFields).toEqual(["age"]);
});

test("Parse SQL Query with standard LIMIT clause", () => {
  const query = "SELECT id, name FROM student LIMIT 2";
  const parsed = parseSELECTQuery(query);
  expect(parsed.limit).toEqual(2);
});

test("Parse SQL Query with large number in LIMIT clause", () => {
  const query = "SELECT id, name FROM student LIMIT 1000";
  const parsed = parseSELECTQuery(query);
  expect(parsed.limit).toEqual(1000);
});

test("Parse SQL Query without LIMIT clause", () => {
  const query = "SELECT id, name FROM student";
  const parsed = parseSELECTQuery(query);
  expect(parsed.limit).toBeNull();
});

test("Parse SQL Query with LIMIT 0", () => {
  const query = "SELECT id, name FROM student LIMIT 0";
  const parsed = parseSELECTQuery(query);
  expect(parsed.limit).toEqual(0);
});

test("Parse SQL Query with negative number in LIMIT clause", () => {
  const query = "SELECT id, name FROM student LIMIT -5";
  const parsed = parseSELECTQuery(query);
  // Assuming the parser sets limit to null for invalid values
  expect(parsed.limit).toBeNull();
});

test("Error Handling with Malformed Query", async () => {
  const query = "SELECT FROM table"; // intentionally malformed
  expect(() => parseSELECTQuery(query)).toThrow(
    "Query parsing error: Invalid SELECT format"
  );
});

test("Parse SQL Query with Basic DISTINCT", () => {
  const query = "SELECT DISTINCT age FROM student";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["age"],
    table: "student",
    isDistinct: true,
    whereClauses: [],
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    orderByFields: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with DISTINCT and Multiple Columns", () => {
  const query = "SELECT DISTINCT student_id, course FROM enrollment";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["student_id", "course"],
    table: "enrollment",
    isDistinct: true,
    whereClauses: [],
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    orderByFields: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with DISTINCT and WHERE Clause", () => {
  const query = 'SELECT DISTINCT course FROM enrollment WHERE student_id = "1"';
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["course"],
    table: "enrollment",
    isDistinct: true,
    whereClauses: [{ field: "student_id", operator: "=", value: '"1"' }],
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    orderByFields: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with DISTINCT and JOIN Operations", () => {
  const query =
    "SELECT DISTINCT student.name FROM student INNER JOIN enrollment ON student.id = enrollment.student_id";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["student.name"],
    table: "student",
    isDistinct: true,
    whereClauses: [],
    groupByFields: null,
    joinType: "inner",
    joinTable: "enrollment",
    joinCondition: {
      left: "student.id",
      right: "enrollment.student_id",
    },
    orderByFields: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with DISTINCT, ORDER BY, and LIMIT", () => {
  const query = "SELECT DISTINCT age FROM student ORDER BY age DESC LIMIT 2";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["age"],
    table: "student",
    isDistinct: true,
    whereClauses: [],
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    orderByFields: [{ fieldName: "AGE", order: "DESC" }],
    limit: 2,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with DISTINCT on All Columns", () => {
  const query = "SELECT DISTINCT * FROM student";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["*"],
    table: "student",
    isDistinct: true,
    whereClauses: [],
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    orderByFields: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with LIKE Clause", () => {
  const query = "SELECT name FROM student WHERE name LIKE '%Jane%'";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["name"],
    table: "student",
    whereClauses: [{ field: "name", operator: "LIKE", value: "%Jane%" }],
    isDistinct: false,
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    orderByFields: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with LIKE Clause and Wildcards", () => {
  const query = "SELECT name FROM student WHERE name LIKE 'J%'";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["name"],
    table: "student",
    whereClauses: [{ field: "name", operator: "LIKE", value: "J%" }],
    isDistinct: false,
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    orderByFields: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with Multiple LIKE Clauses", () => {
  const query =
    "SELECT name FROM student WHERE name LIKE 'J%' AND age LIKE '2%'";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["name"],
    table: "student",
    whereClauses: [
      { field: "name", operator: "LIKE", value: "J%" },
      { field: "age", operator: "LIKE", value: "2%" },
    ],
    isDistinct: false,
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    orderByFields: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Parse SQL Query with LIKE and ORDER BY Clauses", () => {
  const query =
    "SELECT name FROM student WHERE name LIKE '%e%' ORDER BY age DESC";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
    fields: ["name"],
    table: "student",
    whereClauses: [{ field: "name", operator: "LIKE", value: "%e%" }],
    orderByFields: [{ fieldName: "age", order: "DESC" }],
    isDistinct: false,
    groupByFields: null,
    joinType: null,
    joinTable: null,
    joinCondition: null,
    limit: null,
    hasAggregateWithoutGroupBy: false,
    isApproximateCount: false,
    isCountDistinct: false,
    distinctFields: [],
  });
});

test("Testing insert query parser", () => {
  const query = `INSERT INTO grades (student_id, course, grade) VALUES ('4','Physics','A')`;
  const result = parseINSERTQuery(query);
  expect(result).toEqual(
    expect.objectContaining({
      type: "INSERT",
      table: "grades",
      columns: expect.arrayContaining(["student_id", "course", "grade"]),
    })
  );
});

test("Testing Delete query parser", () => {
  const query = `DELETE FROM students where id > 4`;
  const result = parseDELETEQuery(query);
  expect(result).toEqual({
    type: "DELETE",
    table: "students",
    whereClauses: [{ field: "id", operator: ">", value: "4" }],
  });
});

test('Parse SQL Query with APPROXIMATE_COUNT Function', () => {
  const query = "SELECT APPROXIMATE_COUNT(id) FROM student";
  const parsed = parseSELECTQuery(query);
  expect(parsed).toEqual({
      fields: ['count(id)'], // Assuming APPROXIMATE_COUNT is replaced with COUNT for simplicity
      table: 'student',
      whereClauses: [],
      isDistinct: false,
      isApproximateCount: true, // This flag should be true when APPROXIMATE_COUNT is used
      groupByFields: null,
      joinType: null,
      joinTable: null,
      joinCondition: null,
      orderByFields: null,
      limit: null,
      hasAggregateWithoutGroupBy: false,
      isCountDistinct: false,
      distinctFields: []
  });
});
