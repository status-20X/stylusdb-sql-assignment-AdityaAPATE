function parseSELECTQuery(query) {
  try {
    const originalQuery = query;
    query = query.trim();

    const limitSplit = query.split(/\sLIMIT\s(\d+)/i);
    query = limitSplit[0];
    const limitMatch = limitSplit.length > 1 ? limitSplit[1].trim() : null;
    let limit = null;
    if (limitMatch) limit = parseInt(limitMatch);
    let isDistinct = false;
    let isCountDistinct = false;
    let distinctFields = [];

    let isApproximateCount = false;
    const approximateCountRegex = /APPROXIMATE_COUNT\((DISTINCT\s)?(.+?)\)/i;
    const approximateCountMatch = query.match(approximateCountRegex);
    if (approximateCountMatch) {
      // console.log(approximateCountMatch);
      isApproximateCount = true;
      if (approximateCountMatch[1]) {
        isCountDistinct = true;
      }
      let tempVal = approximateCountMatch[1] || "";
      query = query.replace(
        approximateCountRegex,
        `COUNT(${tempVal}${approximateCountMatch[2]})`
      );
      // console.log("This is query", query);
    }

    const countDistinctRegex = /COUNT\((DISTINCT\s\((.*?)\))\)/gi;
    let countDistinctMatch;
    while ((countDistinctMatch = countDistinctRegex.exec(query)) !== null) {
      isCountDistinct = true;
      if (isApproximateCount) {
        distinctFields.push(
          ...countDistinctMatch[2]
            .trim()
            .split(",")
            .map((field) => field.trim())
        );
      } else {
        distinctFields.push(
          ...countDistinctMatch[2]
            .trim()
            .split(",")
            .map((field) => field.trim())
        );
      }
    }

    if (query.toUpperCase().includes("SELECT DISTINCT")) {
      isDistinct = true;
      query = query.toUpperCase().replace("SELECT DISTINCT", "SELECT");
    }

    const orderBySplit = query.split(/\sORDER BY\s(.+)/i);
    query = orderBySplit[0];
    const orderByMatch =
      orderBySplit.length > 1 ? orderBySplit[1].trim() : null;
    let orderByFields = null;
    if (orderByMatch) {
      orderByFields = orderByMatch.split(",").map((field) => {
        const [fieldName, order] = field.trim().split(/\s+/);
        return { fieldName, order: order ? order.toUpperCase() : "ASC" };
      });
    }

    const groupBySplit = query.split(/GROUP BY/i);
    query = groupBySplit[0]; // everything before groupby
    const groupByClause =
      groupBySplit.length > 1 ? groupBySplit[1].trim() : null;

    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0]; // everthing before where
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

    const selectRegex = /^SELECT\s(.+?)\sFROM\s(\S+).*/i;
    const selectMatch = query.match(selectRegex);
    if (!selectMatch) {
      throw new Error(
        "Invalid SELECT clause. Ensure it follows 'SELECT field1, field2 FROM table' format"
      );
    }

    let [, fields, table] = selectMatch;

    //parsing join part if it exists
    const { joinType, joinTable, joinCondition } = parseJoinClause(query);

    let whereClauses = [];
    if (whereClause) whereClauses = parseWhereClauses(whereClause);

    let groupByFields = null;
    if (groupByClause) groupByFields = parseGroupByClause(groupByClause);

    //check for the presence of aggregate fxns without groupby
    const aggregateFunctionRegex =
      /(\bCOUNT\b|\bAVG\b|\bSUM\b|\bMIN\b|\bMAX\b)\s*\(\s*(\*|w+)\s*\)/i;
    const hasAggregateWithoutGroupBy =
      aggregateFunctionRegex.test(originalQuery) && !groupByFields;

    const tempPlaceholder = "__TEMP_COMMA__";
    fields = fields.replace(/\(([^)]+)\)/g, (match) =>
      match.replace(/,/g, tempPlaceholder)
    );
    const parsedFields = fields
      .split(",")
      .map((field) =>
        field
          .trim()
          .toLowerCase()
          .replace(new RegExp(tempPlaceholder, "ig"), ",")
      );
    // console.log(fields,"These are parsed fields: ",parsedFields);

    return {
      table: table.trim().toLowerCase(),
      joinType,
      joinTable,
      joinCondition,
      whereClauses,
      groupByFields,
      isDistinct,
      fields: parsedFields,
      hasAggregateWithoutGroupBy,
      orderByFields,
      limit,
      isApproximateCount,
      isCountDistinct,
      distinctFields,
    };
  } catch (error) {
    throw new Error("Query parsing error: Invalid SELECT format");
  }
}

function parseINSERTQuery(query) {
  const INSERTRegex = /^INSERT\s+INTO\s+(.+)\s+\((.+)\)\s*VALUES\s*\((.+)\)/i;
  const INSERTMatch = query.trim().match(INSERTRegex);
  if (!INSERTMatch) throw new Error("Tnvalid INSERT INTO syntax");

  const [, table, columns, values] = INSERTMatch;
  return {
    type: "INSERT",
    table: table.trim().toLowerCase(),
    columns: columns.split(",").map((column) => column.trim().toLowerCase()),
    values: values.split(",").map((column) => column.trim()),
  };
}

function parseDELETEQuery(query) {
  query = query.trim();
  const whereSplit = query.split(/\sWHERE\s/i);
  query = whereSplit[0]; // everthing before where

  const DELETERegex = /^DELETE\s+FROM\s+(.+)/i;
  const DELETEMatch = query.match(DELETERegex);
  if (!DELETEMatch) throw new Error("Invalid DELETE Query passsed!");

  const [, table] = DELETEMatch;

  const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;
  let whereClauses = [];
  if (whereClause) whereClauses = parseWhereClauses(whereClause);

  return {
    type: "DELETE",
    table: table.trim().toLowerCase(),
    whereClauses,
  };
}

function parseGroupByClause(query) {
  return query.split(",").map((field) => field.trim().toLowerCase());
}

function parseJoinClause(query) {
  const joinRegex =
    /\s(INNER|LEFT|RIGHT)\sJOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
  const joinMatch = query.match(joinRegex);
  // console.log("This is joinMatch: ",joinMatch);

  if (joinMatch)
    return {
      joinType: joinMatch[1].trim().toLowerCase(),
      joinTable: joinMatch[2].trim().toLowerCase(),
      joinCondition: {
        left: joinMatch[3].trim().toLowerCase(),
        right: joinMatch[4].trim().toLowerCase(),
      },
    };
  return {
    joinType: null,
    joinTable: null,
    joinCondition: null,
  };
}

function parseWhereClauses(whereString) {
  const conditionRegex = /(.*?)(=|!=|>=|<=|>|<)(.*)/;
  return whereString.split(/ AND | OR /i).map((conditionString) => {
    let conditionStringtemp = conditionString.toUpperCase();
    if (conditionStringtemp.includes("LIKE")) {
      const [field, pattern] = conditionString.split(/\sLIKE\s/i);
      return {
        field: field.trim().toLowerCase(),
        operator: "LIKE",
        //removes the trailing and starting inverted quotes $1 = the items in th capturing group
        value: pattern.trim().replace(/^'(.*)'$/, "$1"),
      };
    } else {
      const match = conditionString.match(conditionRegex);

      if (match) {
        const [, field, operator, value] = match;
        return {
          field: field.trim().toLowerCase(),
          operator,
          value: value.trim(),
        };
      }
      throw new Error("Invalid WHERE clause format");
    }
  });
}

module.exports = {
  parseSELECTQuery,
  parseJoinClause,
  parseINSERTQuery,
  parseDELETEQuery,
};
