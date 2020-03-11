const conn = require('mysql2-promise')();
const process = require('process');
const {performance} = require('perf_hooks');

const doAsync = false;

// EDIT ME!
conn.configure({host: 'localhost', user: 'root', database: 'test'});

/** Create 2 tables, each with n random fields with n random types */
async function createTables(n) {
  let possibleTypes = ['INTEGER', 'VARCHAR(255)', 'CHAR(72)', 'TEXT'];
  let parentFields = ['id BIGINT UNSIGNED'];
  let childFields = ['id BIGINT UNSIGNED', 'parent_id BIGINT UNSIGNED'];
  function genRandomField(name) {
    return `${name} ${
        possibleTypes[Math.floor(Math.random() * possibleTypes.length)]}`;
  }
  for (let ii = 0; ii < n; ++ii) {
    parentFields.push(genRandomField(`field_${ii}`));
    childFields.push(genRandomField(`field_${ii}`));
  }
  let parentSql = `CREATE TABLE parent (${parentFields.join(',')});`;
  let childSql = `CREATE TABLE child (${childFields.join(',')});`;
  console.log('Creating tables');
  await Promise.all(
      [conn.query(`DROP TABLE parent;`), conn.query(`DROP TABLE child;`)]);
  await Promise.all([
    conn.query(`CREATE TABLE parent (${parentFields.join(',')});`),
    conn.query(`CREATE TABLE child (${childFields.join(',')});`),
    conn.query(`CREATE INDEX parent_index ON child (parent_id);`)
  ]);
}

async function resetTables() {
  await Promise.all(
      [conn.query('TRUNCATE parent;'), conn.query('TRUNCATE child')]);
}

async function seedDb(n, numParents, numChildrenPerParent) {
  const tailArr = [];
  for (let ii = 0; ii < n; ++ii) {
    tailArr.push('NULL');
  }
  const tail = tailArr.join(',');

  let parentInsertData = [];
  for (let ii = 0; ii < numParents; ++ii) {
    parentInsertData.push(`(${ii}, ${tail})`);
  }
  let parentSql = `INSERT INTO parent VALUES ${parentInsertData.join(',')}`;

  let childInsertData = [];
  for (let ii = 0; ii < numParents; ++ii) {
    for (let jj = 0; jj < numChildrenPerParent; ++jj) {
      childInsertData.push(
          `(${jj + ii * numChildrenPerParent}, ${ii}, ${tail})`);
    }
  }
  let childSql = `INSERT INTO child VALUES ${childInsertData.join(',')}`;

  await Promise.all([conn.query(childSql), conn.query(parentSql)]);
}

// return time taken
async function doBench(n, numParents, numChildrenPerParent, pageSize) {
  let offset = 0;
  // Uncomment to choose a random offset, this keeps things colder in cache
  // let offset = Math.floor(Math.random() * (numParents - pageSize));

  // Select random page of parents
  let start = performance.now();
  let res = (await conn.query(
      `SELECT * from parent LIMIT ${offset}, ${pageSize}`))[0];
  if (doAsync) {
    let promises = [];
    for (let ii = 0; ii < res.length; ++ii) {
      promises.push(conn.query(
          `SELECT * FROM child WHERE parent_id = ${res[ii].id} LIMIT 10`));
    }
    await Promise.all(promises);
  } else {
    for (let ii = 0; ii < res.length; ++ii) {
      await conn.query(
          `SELECT * FROM child WHERE parent_id = ${res[ii].id} LIMIT 10`);
    }
  }
  let end = performance.now();
  return end - start;
}

async function main() {
  const n = 10;
  await createTables(n);
  const cases = [
    [10, 100, 10],
    [100, 100, 10],
    [10, 1000, 10],
    [100, 1000, 10],
    [1000, 10, 10],
    [1000, 100, 10],
  ];
  const iter = 10;
  for (const c of cases) {
    let numParents = c[0];
    let numChildrenPerParent = c[1];
    let pageSize = c[2];
    console.log(`Benching ${numParents} parents with ${
        numChildrenPerParent} children per parent, page size ${pageSize}) `);
    await resetTables();
    await seedDb(n, numParents, numChildrenPerParent);
    let min = 10000000000;
    for (let ii = 0; ii < iter; ++ii) {
      let timeTaken =
          await doBench(n, numParents, numChildrenPerParent, pageSize);
      min = Math.min(timeTaken, min);
    }
    console.log(`Min time taken: ${min} millis`);
  }
}

main().then(() => process.exit());
