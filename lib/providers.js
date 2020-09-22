const fs = require('fs');
const path = require('path');

class Providers {
    constructor() {
        for (const file of fs.readdirSync(path.resolve(__dirname, './providers'))) {
            this[path.parse(file).name] = require('./providers/' + file);
        }
    }

    process(source) {
        if (!this[source.provider]) throw new Error(`${source.provider} is not a supported provider`);

        this[source.provider].process(source);
    }
}

module.exports = Providers;
