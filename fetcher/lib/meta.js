const { VERBOSE } = require('./utils');

const {
    getObject,
    putObject
} = require('./utils');

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
            const body = await getObject(this.props.Bucket, this.props.Key);
            return JSON.parse(body);
        } catch (err) {
            if (err.statusCode !== 404)
                throw err;
            if (VERBOSE)
                console.log('No meta file found.');
            return undefined;
        }
    }

    save(body) {
        return putObject(
            JSON.stringify(body),
            this.props.Bucket,
            this.props.Key,
        );
    }
}
exports.MetaDetails = MetaDetails;
