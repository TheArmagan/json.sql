import "dotenv/config";
import jp from "jsonpath";
import { Pool, Client, escapeLiteral, escapeIdentifier } from "pg";
import assert from "node:assert";
import _ from "lodash";

const NUMBER_REGEX = /^\d+$/;

const createTableQueryString = (tableName: string) => {
  const escapedTableName = escapeIdentifier(tableName);
  return `
    CREATE TABLE IF NOT EXISTS ${escapedTableName} (
      name TEXT NOT NULL,
      path TEXT NOT NULL,
      data JSONB
    );

    DO $$
    BEGIN
      BEGIN
        CREATE UNIQUE INDEX ${escapeIdentifier(`${tableName}_name_path_unq`)} ON ${escapedTableName} (name, path);
      EXCEPTION WHEN DUPLICATE_TABLE THEN
        -- Do nothing, accept existing table
      END;

      BEGIN 
        CREATE INDEX ${escapeIdentifier(`${tableName}_name_idx`)} ON ${escapedTableName} (name);
      EXCEPTION WHEN DUPLICATE_TABLE THEN
        -- Do nothing, accept existing table
      END;

      BEGIN
        CREATE INDEX ${escapeIdentifier(`${tableName}_path_idx`)} ON ${escapedTableName} (path);
      EXCEPTION WHEN DUPLICATE_TABLE THEN
        -- Do nothing, accept existing table
      END;
    END $$;

  `;
};

(async () => {

  const pool = new Pool({
    host: "192.168.1.110",
    port: 5432,
    user: "postgres",
    database: "json-sql",
    password: process.env.DB_PASS,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000
  });

  const client = await pool.connect();

  async function getKeys() {
    return client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE';
    `).then((res) => res.rows.map((row) => row.table_name));
  }

  function assertRawPathExp(exp: any, extra: string) {
    assert.equal(exp.operation, "member", `Operation must be member. (${extra})`);
    assert.equal(exp.scope, "child", `Scope must be child. (${extra})`);
    assert.equal(exp.expression.type, "identifier", `Expression must be a identifier expression. (${extra})`);
  }

  function buildBaseQueryParams(jsonPath: string): BaseQueryParams {
    const parsedPath = jp.parse(jsonPath);

    assert.ok(parsedPath.length >= 1, `JSONPath must have one or more operations. (${jsonPath})`);
    assertRawPathExp(parsedPath[0], `Root expression (${jsonPath})`);

    const tableName = `${parsedPath[0].expression.value}`;
    const rowName = parsedPath[1] ? `${parsedPath[1].expression.value}` : null;
    const path = parsedPath[2] ? parsedPath.slice(2) : [];

    return { tableName, rowName, path };
  }

  function buildInsertQuery(baseParams: BaseQueryParams, value: any): string {
    const basePath = baseParams.path.map((part) => part.expression.value);
    const pathCombinations = getPathCombinations(value);

    const inserts = pathCombinations.map(([path, data]) => {
      const pathString = toJsonPath(basePath.concat(path));
      const dataString = escapeLiteral(JSON.stringify(data));

      return `
        INSERT INTO ${escapeIdentifier(baseParams.tableName)} (name, path, data)
        VALUES (${escapeLiteral(baseParams.rowName!)}, ${escapeLiteral(pathString)}, ${dataString})
        ON CONFLICT (name, path) DO UPDATE
        SET data = ${dataString};
      `;
    });

    const transaction = `
      BEGIN;
      ${inserts.join("\n")}
      COMMIT;
    `;

    return transaction;
  }

  function buildSelectQuery(baseParams: BaseQueryParams): string {
    let pathFilter = "";
    let pathAccumulator: string[] = [];
    let shouldPrependWildcard = false;
    baseParams.path.forEach((part) => {
      switch (part.expression.type) {
        case "identifier":
        case "numeric_literal":
          pathAccumulator.push(part.expression.value);
          shouldPrependWildcard = true;
          break;
        case "wildcard":
          pathFilter += `${toJsonPath(pathAccumulator).replace(/\./g, "\\.")}[.*]`;
          pathAccumulator = [];
          shouldPrependWildcard = false;
          break;
      }
    });

    if (pathAccumulator.length) pathFilter += toJsonPath(pathAccumulator).replace(/\./g, "\\.");
    if (baseParams.rowName === "*" && shouldPrependWildcard) pathFilter = `.*${pathFilter}.*`;

    return `
      SELECT *
      FROM ${escapeIdentifier(baseParams.tableName)}
      WHERE ${baseParams.rowName === "*" ? "TRUE" : `name = ${escapeLiteral(baseParams.rowName!)}`}
      ${pathFilter ? ` AND path SIMILAR TO '${pathFilter}'` : ""};
    `;
  }

  async function set(jsonPath: string, value: any) {
    const baseParams = buildBaseQueryParams(jsonPath);
    const queryObj: string[] = [
      createTableQueryString(baseParams.tableName)
    ];
    if (baseParams.rowName) {
      queryObj.push(
        buildInsertQuery(baseParams, value)
      );
    } else if (Array.isArray(value) || typeof value === "object") {
      queryObj.push(
        ...Object.entries(value).map(([key, val]) => {
          return buildInsertQuery({ ...baseParams, rowName: key }, val);
        })
      );
    } else {
      queryObj.push(
        buildInsertQuery({ ...baseParams, rowName: "__" }, value)
      );
    }

    console.log(queryObj.join("\n"));

    await pool.query(queryObj.join("\n"));
  }

  async function get(jsonPath: string) {
    const baseParams = buildBaseQueryParams(jsonPath);
    const query = buildSelectQuery(baseParams);

    console.log(query);

    const res = await pool.query(query);

    switch (res.rowCount) {
      case 0:
        return null;
      case 1:
        return res.rows[0].data;
      default: {
        const hasManyKeys = Object.keys(_.groupBy(res.rows, "name")).length > 1;
        const isArr = res.rows.every((row) => NUMBER_REGEX.test(row.name));
        let result: any = isArr ? [] : {};
        const pathString = toJsonPath(baseParams.path.map((part) => part.expression.value));
        res.rows.forEach((row) => {
          let path = row.path.startsWith(pathString) ? row.path.replace(pathString, "") : row.path;
          path = toJsonPath([isArr ? Number(row.name) : row.name, ...fromJsonPath(path)].filter(i => i !== ""));
          _.set(result, path, row.data);
        });
        return hasManyKeys ? result : Object.values(result)[0];
      }
    }
  }

  await set(`users[0]`, {
    name: "John",
    age: 30,
    address: {
      city: "New York",
      state: "NY"
    }
  });

  await set(`users[1]`, {
    name: "Jane",
    age: 25,
    address: {
      city: "San Francisco",
      state: "CA"
    }
  });


  await set(`usersAllInOnce`, [
    {
      name: "John",
      age: 30,
      address: {
        city: "New York",
        state: "NY"
      }
    },
    {
      name: "Jane",
      age: 25,
      address: {
        city: "San Francisco",
        state: "CA"
      }
    }
  ]);

  await set("test", "hello world");


  console.log(
    await get("users[*].address"),
  );

  await client.release();
  await pool.end();
})();

interface PathPart {
  operation: string,
  scope: string,
  expression: {
    type: string,
    value: any
  }
}

interface BaseQueryParams {
  tableName: string,
  rowName: string | null,
  path: PathPart[]
}

function getPathCombinations(obj: any, prefix: any[] = []): [(string | number)[], any][] {
  let paths: any[] = [];

  if (typeof obj === "object" && obj !== null) {
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) {
        paths = paths.concat(getPathCombinations(obj[i], prefix.concat(i)));
      }
    } else {
      for (const key in obj) {
        if (key in obj) {
          paths = paths.concat(getPathCombinations(obj[key], prefix.concat(key)));
        }
      }
    }
  } else {
    paths.push([prefix, obj]);
  }

  return paths;
}

// pre-compiling regexes for performance
const NORMAL_KEY_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
function toJsonPath(pathArray: (string | number)[]): string {
  return pathArray.map(key => {
    if (typeof key === "number") {
      return `[${key}]`;
    } else if (NORMAL_KEY_REGEX.test(key)) {
      return `.${key}`;
    } else {
      return `["${key.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"]`;
    }
  }).join("").replace(/^\./, "");
}

const FROM_JSON_REGEX = /(\w+)|\[(\d+|".*?")\]/g;
function fromJsonPath(jsonPath: string) {
  const matches = [...jsonPath.matchAll(FROM_JSON_REGEX)];
  return matches.map(match => {
    if (typeof match[1] !== "undefined") {
      return match[1];
    }
    if (typeof match[2] !== "undefined") {
      const value = match[2];
      if (/^\d+$/.test(value)) {
        return parseInt(value, 10);
      } else {
        return value.slice(1, -1).replace(/\\"/g, '"').replace(/\\\\/g, '\\');
      }
    }
  }) as (string | number)[];
}
