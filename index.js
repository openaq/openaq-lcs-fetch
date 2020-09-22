const fs = require('fs');
const path = require('path');

if (require.main === module) {
    handler();
}

function handler(event) {
    const source_name = process.env.SOURCE || event.Records[0].body;
    if (!source_name) throw new Error('SOURCE env var or event required');

    const source = JSON.parse(fs.readFileSync(path.resolve(__dirname, './sources/', source_name + '.json')));

    console.error(source)

    return {};
}

module.exports.handler = handler;
