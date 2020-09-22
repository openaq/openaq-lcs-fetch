const fs = require('fs');
const path = require('path');

if (require.main === module) {
    handler();
}

function handler() {
    if (!process.env.SOURCE) throw new Error('SOURCE env var required');

    const source = JSON.parse(fs.readFileSync(path.resolve(__dirname, './sources/', process.env.SOURCE + '.json')));

    console.error(source);
}

module.exports.handler = handler;
