const path = process.env.ENV ? `.env.${process.env.ENV}` : '.env';
require('dotenv').config({ path });

const providers = new (require('./lib/providers'))();
const sources = require('./sources');


if (require.main === module) {
    handler();
}

async function handler(event) {
    try {

        if (!process.env.SOURCE && !event)
            throw new Error('SOURCE env var or event required');

        if (!process.env.BUCKET)
            throw new Error('BUCKET env var required');

        if (!process.env.STACK)
            process.env.STACK = 'local';

        const source_name = process.env.SOURCE || event.Records[0].body;
        const source = sources.find((source) => source.provider === source_name);
        if (!source) throw new Error(`Unable to find ${source_name} in sources.`);

        const log = await providers.processor(source);
        await providers.publish(log, 'fetcher/success');
        return log;
    } catch (err) {
        providers.publish(err, 'fetcher/error');
        process.exit(1);
    }
}

module.exports.handler = handler;
