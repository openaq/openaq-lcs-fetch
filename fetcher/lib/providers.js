const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const {
    VERBOSE,
    DRYRUN,
    gzip,
    unzip,
    prettyPrintStation
} = require('./utils');

const s3 = new AWS.S3({
    maxRetries: 10
});

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
            if (currentData === newData && !process.env.FORCE) {
                if (VERBOSE) console.log(`station has not changed - station: ${providerStation}`);
                return;
            }
            if (VERBOSE) {
                console.log(`-------------------------\nUpdate ${providerStation}\n----------------------> to:`);
                prettyPrintStation(newData);
                console.log('-----------------> from');
                prettyPrintStation(currentData);
            }
        } catch (err) {
            if (err.statusCode !== 404) throw err;
        }

        const compressedString = await gzip(newData);
        if (!DRYRUN) {
            if (VERBOSE) console.debug(`Saving station to ${Bucket}/${Key}`);
            await s3.putObject({
                Bucket,
                Key,
                Body: compressedString,
                ContentType: 'application/json',
                ContentEncoding: 'gzip'
            }).promise();
        }
        if (VERBOSE) console.log(`finished station: ${providerStation}\n------------------------`);
    }

    /**
     * Given a measures object, save it to s3
     *
     * @param {string} provider The name of the provider (ie purpleair)
     * @param {Measures} measures A measurements object of measures
     * @param {string} id An optional identifier to use when creating filename
     */
    static async put_measures(provider, measures, id) {
        if (!measures.length) {
            return console.warn('No measures found, not uploading to S3.');
        }
        const Bucket = process.env.BUCKET;
        const filename = id || `${Math.floor(Date.now() / 1000)}-${Math.random().toString(36).substring(8)}`;
        const Key = `${process.env.STACK}/measures/${provider}/${filename}.csv.gz`;
        const compressedString = await gzip(measures.csv());
        if (DRYRUN) {
            console.log(`Would have saved ${measures.length} measurements to '${Bucket}/${Key}'`);
            return new Promise((y) => y(true));
        }
        if (VERBOSE) console.debug(`Saving measurements to ${Bucket}/${Key}`);
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
