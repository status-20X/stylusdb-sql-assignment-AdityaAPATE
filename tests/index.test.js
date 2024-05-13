// tests/index.test.js

const executeSELECTQuery = require('../src/index');

test('Execute SQL Query with WHERE Clause', async () => {
    const query = 'SELECT id, name FROM sample WHERE age = 25';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBe(1);
    expect(result[0]).toHaveProperty('id');
    expect(result[0]).toHaveProperty('name');
    expect(result[0].id).toBe('2');
});