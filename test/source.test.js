const tape = require('tape');
const Ajv = require('ajv');
const schema = require('../schema/v1.json');
const sources = require('../fetcher/sources');

const ajv = new Ajv({
    schemaId: 'auto'
});

const validate = ajv.compile(schema);

tape('validate', (t) => {
    t.ok(validate, 'schema loaded');
    t.end();
});


// find all the sources, has to be synchronous for tape
sources.forEach((source) => {
	  tape(`tests for ${source}`, (t) => {
        try {
            const valid = validate(source);

            t.ok(valid, `${source.provider}: ${JSON.stringify(validate.errors)}`);
        } catch (err) {
            t.fail(`could not parse ${source} as JSON: ${err}`);
        }

        t.end();
    });
});
