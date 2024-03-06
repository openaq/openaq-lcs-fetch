const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

const sqs = new SQSClient();

async function handler() {
    if (!process.env.QUEUE_URL)
        throw new Error('QUEUE_URL env var required');
    if (!process.env.SOURCES)
        throw new Error('SOURCES env var required');

    for (const source of process.env.SOURCES.split(',')) {
        try {
            const cmd = new SendMessageCommand({
                MessageBody: source,
                QueueUrl: process.env.QUEUE_URL
            });
            await sqs.send(cmd);
            console.log(`Inserted '${source}' into queue`);
        } catch (err) {
            console.error(`Failed to send message for ${source}: ${err}`);
        }
    }
}

module.exports.handler = handler;
