'use strict';
const tape = require('tape');
const glob = require('glob');
const fs = require('fs');
const Ajv = require('ajv');
const schema = require('../schema/v1.json');

const ajv = new Ajv({
    schemaId: 'auto'
});

ajv.addMetaSchema(require('ajv/lib/refs/json-schema-draft-04.json'), "http://json-schema.org/draft-04/schema#");

const validate = ajv.compile(schema);

tape('validate', (t) => {
    t.ok(validate, 'schema loaded');
    t.end();
});

// find all the sources, has to be synchronous for tape
glob.sync('../sources/**.json').forEach((source) => {
    tape(`tests for ${source}`, (t) => {
        try {
            const data = JSON.parse(fs.readFileSync(source, 'utf8'));

            const valid = validate(data);

            t.ok(valid, `${source}: ${JSON.stringify(validate.errors)}`);
        } catch (err) {
            t.fail(`could not parse ${source} as JSON: ${err}`);
        }

        t.end();
    });
});
