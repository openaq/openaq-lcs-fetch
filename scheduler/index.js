'use strict';
const AWS = require('aws-sdk');

const sqs = new AWS.SQS();

async function handler() {
    if (!process.env.QUEUE)
        throw new Error('QUEUE env var required');
    if (!process.env.SOURCES)
        throw new Error('SOURCES env var required');

    const sources = process.env.SOURCES.split(',');
    send(sources);
}

function send(sources) {
    if (!sources.length) return;

    sqs.sendMessage({
        MessageBody: sources.pop(),
        QueueUrl: process.env.QUEUE
    }, (err) => {
        if (err) console.error(err);

        send(sources);
    });
}

module.exports.handler = handler;
