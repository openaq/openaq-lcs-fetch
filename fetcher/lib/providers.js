const fs = require('fs');
const zlib = require('zlib');
const { promisify } = require('util');
const path = require('path');
const AWS = require('aws-sdk');
const { VERBOSE } = require('./utils');

const s3 = new AWS.S3({
    maxRetries: 10
});
const gzip = promisify(zlib.gzip);
const unzip = promisify(zlib.unzip);

/**
 * Runtime handler for each of the custom provider scripts, as well
 * as helper functions for pushing standardized data to S3
 */
class Providers {
    constructor() {
        const providersDir = '../providers';
        for (const file of fs.readdirSync(path.resolve(__dirname, providersDir))) {
            this[path.parse(file).name] = require(`${providersDir}/${file}`);
        }
    }

    /**
     * Given a source config file, choose the corresponding provider script to run
     *
     * @param {String} source_name
     * @param {Object} source
     */
    async processor(source_name, source) {
        if (!this[source.provider]) throw new Error(`${source.provider} is not a supported provider`);

        await this[source.provider].processor(source_name, source);
    }

    /**
     * Push an array of stations to S3
     *
     * @param {String} provider The name of the provider
     * @param {SensorNode[]} stations Stations to push to s3
     *
     * @returns {Promise}
     */
    static async put_stations(provider, stations) {
        for (const sta of stations) {
            await this.put_station(provider, sta);
        }

        return true;
    }

    /**
     * Push a single station to S3
     * @param {String} provider The name of the provider
     * @param {SensorNode} station Station to push to s3
     */
    static async put_station(provider, station) {
        const Bucket = process.env.BUCKET;
        const providerStation = `${provider}/${station.sensor_node_id}`;
        const Key = `${process.env.STACK}/stations/${providerStation}.json.gz`;

        const newData = JSON.stringify(station.json());

        // Diff data to minimize costly S3 PUT operations
        try {
            const resp = await s3.getObject({ Bucket, Key }).promise();
            const currentData = (await unzip(resp.Body)).toString('utf-8');
            if (currentData === newData) {
                if (VERBOSE) console.log(`skip - station: ${providerStation}`);
                return;
            }
            if (VERBOSE) console.log(
                `Update ${providerStation}\n  from:\n    ${currentData}\n  to:\n    ${newData}`
            );
        } catch (err) {
            if (err.statusCode !== 404) throw err;
        }

        const compressedString = await gzip(newData);
        await s3.putObject({
            Bucket,
            Key,
            Body: compressedString,
            ContentType: 'application/json',
            ContentEncoding: 'gzip'
        }).promise();
        if (VERBOSE) console.log(`ok - station: ${providerStation}`);
    }

    /**
     * Given a measures object, save it to s3
     *
     * @param {string} provider The name of the provider (ie purpleair)
     * @param {Measures} measures A measurements object of measures
     */
    static async put_measures(provider, measures) {
        if (!measures.length)
            return console.warn('No measures found, not uploading to S3.');
        const Bucket = process.env.BUCKET;
        const Key = `${process.env.STACK}/measures/${provider}/${Math.floor(Date.now() / 1000)}.csv.gz`;
        const compressedString = await gzip(measures.csv());
        return s3.putObject({
            Bucket,
            Key,
            Body: compressedString,
            ContentType: 'text/csv',
            ContentEncoding: 'gzip'
        }).promise();
    }
}

module.exports = Providers;
