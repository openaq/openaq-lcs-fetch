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
      const source = sources.find((source) => source.name === source_name);

      if (!source) throw new Error(`Unable to find ${source_name} in sources.`);

      console.log(`Processing ${process.env.STACK}: '${source.provider}/${source.name}'`);
      await providers.processor(source);

        return {};
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
}

module.exports.handler = handler;
