'use strict';

const fs = require('fs');
const path = require('path');
const providers = new (require('./lib/providers'))();

if (require.main === module) {
    handler();
}

async function handler(event) {
    try {
        if (!process.env.SOURCE && !event) throw new Error('SOURCE env var or event required');

        const source_name = process.env.SOURCE || event.Records[0].body;

        const source = JSON.parse(fs.readFileSync(path.resolve(__dirname, './sources/', source_name + '.json')));

        await providers.process(source);

        return {};
    } catch (err) {
        console.error(err.message);
        process.exit(1);
    }
}

module.exports.handler = handler;
