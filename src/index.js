const { readCSV, readCSVForHLL, writeCSV } = require("./csvStorage.js");
const {
  parseSELECTQuery,
  parseINSERTQuery,
  parseDELETEQuery,
} = require("./queryParser.js");
const hll = require("hll");

async function executeSELECTQuery(query) {
  try {
    const {
      fields,
      table,
      whereClauses,
      joinType,
      joinTable,
      joinCondition,
      groupByFields,
      hasAggregateWithoutGroupBy,
      orderByFields,
      limit,
      isDistinct,
      isApproximateCount,
      isCountDistinct,
      distinctFields,
    } = parseSELECTQuery(query); //id,name
    if (
      isApproximateCount &&
      fields.length === 1 &&
      fields[0].toUpperCase() === "COUNT(*)" &&
      whereClauses.length === 0
    ) {
      let hll = await readCSVForHLL(`${table}.csv`);
      return [{ "APPROXIMATE_COUNT(*)": hll.estimate() }];
    }
    //fails when file is not found
    let data = await readCSV(`${table}.csv`);

    //now first data needs to handle join condition
    //then filter on where clause
    if (joinType && joinTable && joinCondition) {
      const joinData = await readCSV(`${joinTable}.csv`);
      switch (joinType.toUpperCase()) {
        case "INNER":
          data = performInnerJoin(data, joinData, joinCondition, fields, table);
          break;
        case "LEFT":
          data = performLeftJoin(data, joinData, joinCondition, fields, table);
          break;
        case "RIGHT":
          data = performRightJoin(data, joinData, joinCondition, fields, table);
          break;
      }
    }

    //where domain
    //filter the data before returning result based on where
    const filteredData =
      whereClauses.length > 0 //every assuming only AND is given
        ? data.filter((row) =>
            whereClauses.every((clause) => evaluateCondition(row, clause))
          )
        : data;
    data = filteredData;
    // grouby domain
    let groupResults = data;
    if (hasAggregateWithoutGroupBy) {
      const tempResult = {};

      fields.forEach((field) => {
        const match = /(\w+)\((\*|\w+)\)/.exec(field);
        if (match) {
          const [, aggFunc, aggField] = match;
          switch (aggFunc.toUpperCase()) {
            case "COUNT":
              result[field] = filteredData.length;
              break;
            case "SUM":
              result[field] = filteredData.reduce(
                (acc, row) => acc + parseFloat(row[aggField]),
                0
              );
              break;
            case "AVG":
              result[field] =
                filteredData.reduce(
                  (acc, row) => acc + parseFloat(row[aggField]),
                  0
                ) / filteredData.length;
              break;
            case "MIN":
              result[field] = Math.min(
                ...filteredData.map((row) => parseFloat(row[aggField]))
              );
              break;
            case "MAX":
              result[field] = Math.max(
                ...filteredData.map((row) => parseFloat(row[aggField]))
              );
              break;
            // Additional aggregate functions can be handled here
          }
        }
      });
      // applying distinct
      if (isDistinct) tempResult = { ...applyDistinct(tempResult, fields) };

      //sorting - order by
      if (orderByFields) {
        tempResult = {
          ...[tempResult].sort((a, b) => {
            for (let { fieldName, order } of orderByFields) {
              if (a[fieldName] < b[fieldName]) return order === "ASC" ? -1 : 1;
              if (a[fieldName] > b[fieldName]) return order === "ASC" ? 1 : -1;
            }
            return 0;
          }),
        };
      }
      // applying limit
      if (limit !== null) {
        return [tempResult].slice(0, limit);
      }
      return [tempResult];
    } else if (groupByFields) {
      groupResults = applyGroupBy(filteredData, groupByFields, fields);
      if (isDistinct) groupResults = applyDistinct(groupResults, fields);

      if (orderByFields) {
        groupResults.sort((a, b) => {
          for (let { fieldName, order } of orderByFields) {
            if (a[fieldName] < b[fieldName]) return order === "ASC" ? -1 : 1;
            if (a[fieldName] > b[fieldName]) return order === "ASC" ? 1 : -1;
          }
          return 0;
        });
      }

      if (limit !== null) return groupResults.slice(0, limit);
      return groupResults;
    }
    data = groupResults;

    if (isCountDistinct) {
      if (isApproximateCount) {
        let h = hll({ bitSampleSize: 12, digestSize: 128 });
        data.forEach((row) =>
          h.insert(distinctFields.map((field) => row[field]).join("|"))
        );
        // console.log("This is: ",[{ [`APPROXIMATE_${fields[0]}`]: h.estimate() }])
        return [{ [`APPROXIMATE_${fields[0]}`]: h.estimate() }];
      } else {
        let distinctResults = [
          ...new Map(
            data.map((item) => [
              distinctFields.map((field) => item[field]).join("|"),
              item,
            ])
          ).values(),
        ];
        // console.log("This is :",[{ [fields[0]]: distinctResults.length }])
        return [{ [fields[0]]: distinctResults.length }];
      }
    }
    //select
    const result = [];
    data.forEach((row) => {
      const filteredRow = {};
      // what if the asked field is not in the data? handled => []
      fields.forEach((field) => {
        if (row[field] !== undefined) filteredRow[field] = row[field];
      });
      if (Object.keys(filteredRow).length !== 0) result.push(filteredRow);
    }); // [] of {id,name}
    if (isDistinct) data = applyDistinct(result, fields);
    else data = result;
    // order by
    if (orderByFields) {
      data.sort((a, b) => {
        for (let { fieldName, order } of orderByFields) {
          if (a[fieldName] < b[fieldName]) return order === "ASC" ? -1 : 1;
          if (a[fieldName] > b[fieldName]) return order === "ASC" ? 1 : -1;
        }
        return 0;
      });
    }

    //limit
    if (limit !== null) return result.slice(0, limit);
    return data;
  } catch (error) {
    throw new Error(`Error executing query: ${error.message}`);
  }
}

async function executeINSERTQuery(query) {
  const { table, columns, values } = parseINSERTQuery(query);

  //currently whole file is loaded into memory and then written back into file
  const data = await readCSV(`${table}.csv`);

  const newRow = {};
  columns.forEach((column, index) => {
    let value = values[index];
    if (value.startsWith("'") && value.endsWith("'"))
      value = value.substring(1, value.length - 1);
    newRow[column] = value;
  });

  data.push(newRow);

  await writeCSV(`${table}.csv`, data);

  return { message: `Row inserted Successfully` };
}

async function executeDELETEQuery(query) {
  const { type, table, whereClauses } = parseDELETEQuery(query);
  let data = await readCSV(`${table}.csv`);

  if (whereClauses.length > 0) {
    // rows which satisfy the where clauses will be removed, hence must return false value, hence ! used
    data = data.filter(
      (row) => !whereClauses.every((clause) => evaluateCondition(row, clause))
    );
  } else {
    // no wherclause provided so delete entire table
    data = [];
  }
  await writeCSV(`${table}.csv`, data);
  return { message: "Rows deleted successfully." };
}

function applyDistinct(data, fields) {
  return [
    ...new Map(
      data.map((item) => [fields.map((field) => item[field]).join("|"), item])
    ).values(),
  ];
}

function evaluateCondition(row, clause) {
  let { field, operator, value } = clause;

  // Check if the field exists in the row
  if (row[field] === undefined) {
    throw new Error(`Invalid field: ${field}`);
  }

  // Parse row value and condition value based on their actual types
  const rowValue = parseValue(row[field]);
  let conditionValue = parseValue(value);

  if (operator === "LIKE") {
    const regexPattern =
      "^" + value.replace(/%/g, ".*").replace(/_/g, ".") + "$";
    const regex = new RegExp(regexPattern, "i");
    return regex.test(rowValue);
  }

  // console.log("EVALUATING", rowValue, operator, conditionValue, typeof (rowValue), typeof (conditionValue));

  switch (operator) {
    case "=":
      return rowValue === conditionValue;
    case "!=":
      return rowValue !== conditionValue;
    case ">":
      return rowValue > conditionValue;
    case "<":
      return rowValue < conditionValue;
    case ">=":
      return rowValue >= conditionValue;
    case "<=":
      return rowValue <= conditionValue;
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}
// Helper function to parse value based on its apparent type
function parseValue(value) {
  // Return null or undefined as is
  if (value === null || value === undefined) {
    return value;
  }

  // If the value is a string enclosed in single or double quotes, remove them
  if (
    typeof value === "string" &&
    ((value.startsWith("'") && value.endsWith("'")) ||
      (value.startsWith('"') && value.endsWith('"')))
  ) {
    value = value.substring(1, value.length - 1);
  }

  // Check if value is a number
  if (!isNaN(value) && value.trim() !== "") {
    return Number(value);
  }
  // Assume value is a string if not a number
  return value;
}

function applyGroupBy(data, groupByFields, aggregateFunctions) {
  const groupResults = {};

  data.forEach((row) => {
    // Generate a key for the group
    const groupKey = groupByFields.map((field) => row[field]).join("-");

    // Initialize group in results if it doesn't exist
    if (!groupResults[groupKey]) {
      groupResults[groupKey] = { count: 0, sums: {}, mins: {}, maxes: {} };
      groupByFields.forEach(
        (field) => (groupResults[groupKey][field] = row[field])
      );
    }

    // Aggregate calculations
    groupResults[groupKey].count += 1;
    aggregateFunctions.forEach((func) => {
      const match = /(\w+)\((\w+)\)/.exec(func);
      if (match) {
        const [, aggFunc, aggField] = match;
        const value = parseFloat(row[aggField]);

        switch (aggFunc.toUpperCase()) {
          case "SUM":
            groupResults[groupKey].sums[aggField] =
              (groupResults[groupKey].sums[aggField] || 0) + value;
            break;
          case "MIN":
            groupResults[groupKey].mins[aggField] = Math.min(
              groupResults[groupKey].mins[aggField] || value,
              value
            );
            break;
          case "MAX":
            groupResults[groupKey].maxes[aggField] = Math.max(
              groupResults[groupKey].maxes[aggField] || value,
              value
            );
            break;
          // Additional aggregate functions can be added here
        }
      }
    });
  });

  // Convert grouped results into an array format
  return Object.values(groupResults).map((group) => {
    // Construct the final grouped object based on required fields
    const finalGroup = {};
    groupByFields.forEach((field) => (finalGroup[field] = group[field]));
    aggregateFunctions.forEach((func) => {
      const match = /(\w+)\((\*|\w+)\)/.exec(func);
      if (match) {
        const [, aggFunc, aggField] = match;
        switch (aggFunc.toUpperCase()) {
          case "SUM":
            finalGroup[func] = group.sums[aggField];
            break;
          case "MIN":
            finalGroup[func] = group.mins[aggField];
            break;
          case "MAX":
            finalGroup[func] = group.maxes[aggField];
            break;
          case "COUNT":
            finalGroup[func] = group.count;
            break;
          // Additional aggregate functions can be handled here
        }
      }
    });

    return finalGroup;
  });
}

function performInnerJoin(data, joinData, joinCondition, fields, table) {
  data = data.flatMap((mainRow) => {
    return joinData
      .filter((joinRow) => {
        const mainValue = mainRow[joinCondition.left.split(".")[1]];
        const joinValue = joinRow[joinCondition.right.split(".")[1]];
        return mainValue === joinValue;
      })
      .map((joinRow) => {
        return fields.reduce((acc, field) => {
          const [tableName, fieldName] = field.split(".");
          acc[field] =
            tableName === table ? mainRow[fieldName] : joinRow[fieldName];
          return acc;
        }, {});
      });
  });
  return data;
}

function performLeftJoin(data, joinData, joinCondition, fields, table) {
  return data.flatMap((mainRow) => {
    const matchingJoinRows = joinData.filter((joinRow) => {
      const mainValue = mainRow[joinCondition.left.split(".")[1]];
      const joinValue = joinRow[joinCondition.right.split(".")[1]];
      return mainValue === joinValue;
    });

    if (matchingJoinRows.length === 0)
      // add the main row with null values on joinTable
      return [createResultRow(mainRow, null, fields, table, true)];

    return matchingJoinRows.map((joinRow) =>
      createResultRow(mainRow, joinRow, fields, table, true)
    );
  });
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
  // Cache the structure of a main table row (keys only)
  const mainTableRowStructure =
    data.length > 0
      ? Object.keys(data[0]).reduce((acc, key) => {
          acc[key] = null; // Set all values to null initially
          return acc;
        }, {})
      : {};

  return joinData.map((joinRow) => {
    const mainRowMatch = data.find((mainRow) => {
      const mainValue = getValueFromRow(mainRow, joinCondition.left);
      const joinValue = getValueFromRow(joinRow, joinCondition.right);
      return mainValue === joinValue;
    });

    // Use the cached structure if no match is found
    const mainRowToUse = mainRowMatch || mainTableRowStructure;

    // Include all necessary fields from the 'student' table
    return createResultRow(mainRowToUse, joinRow, fields, table, true);
  });
}

// how workks?
function getValueFromRow(row, compoundFieldName) {
  const [tableName, fieldName] = compoundFieldName.split(".");
  return row[`${tableName}.${fieldName}`] || row[fieldName];
}

//how workks?
// helper fxn to create a result row in right and left join
function createResultRow(
  mainRow,
  joinRow,
  fields,
  table,
  includeAllMainFields
) {
  const resultRow = {};

  if (includeAllMainFields) {
    // include all fields from the main table
    Object.keys(mainRow || {}).forEach((key) => {
      const prefixedKey = `${table}.${key}`;
      resultRow[prefixedKey] = mainRow ? mainRow[key] : null;
    });
  }

  fields.forEach((field) => {
    const [tableName, fieldName] = field.includes(".")
      ? field.split(".")
      : [table, field];
    resultRow[field] =
      tableName === table && mainRow
        ? mainRow[fieldName]
        : joinRow
        ? joinRow[fieldName]
        : null;
  });
  // console.log("This is result row: ",resultRow);
  return resultRow;
}

module.exports = { executeSELECTQuery, executeINSERTQuery, executeDELETEQuery };
