const { VERBOSE } = require('./utils');
const AWS = require('aws-sdk');
const s3 = new AWS.S3({
    maxRetries: 10
});

/**
 * Helper to store metadata about a source in S3.
 */
class MetaDetails {
    constructor(source_name) {
        this.source_name = source_name;
    }
    get props() {
        return {
            Bucket: process.env.BUCKET,
            Key: `${process.env.STACK}/meta/${this.source_name}`
        };
    }

    async load() {
        try {
            const resp = await s3.getObject(this.props).promise();
            return JSON.parse(resp.Body.toString('utf-8'));
        } catch (err) {
            if (err.statusCode !== 404)
                throw err;
            if (VERBOSE)
                console.log('No meta file found.');
            return undefined;
        }
    }

    save(body) {
        return s3.putObject({
            ...this.props,
            Body: JSON.stringify(body),
            ContentType: 'application/json'
        }).promise();
    }
}
exports.MetaDetails = MetaDetails;
