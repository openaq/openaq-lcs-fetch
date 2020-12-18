const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const s3 = new AWS.S3({
    maxRetries: 10
});

/**
 * Runtime handler for each of the custom provider scripts, as well
 * as helper functions for pushing standardized data to S3
 */
class Providers {
    constructor() {
        for (const file of fs.readdirSync(path.resolve(__dirname, './providers'))) {
            this[path.parse(file).name] = require('./providers/' + file);
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
        const Key = `${process.env.STACK}/stations/${providerStation}`;

        const newData = JSON.stringify(station.json());

        // Diff data to minimize costly S3 PUT operations
        try {
            const resp = await s3.getObject({ Bucket, Key }).promise();
            const currentData = resp.Body.toString('utf-8');
            if (currentData === newData) {
                console.log(`skip - station: ${providerStation}`);
                return;
            }
            console.log(
                `Update ${providerStation}\n  from:\n    ${currentData}\n  to:\n    ${newData}`
            );
        } catch (err) {
            if (err.statusCode !== 404) throw err;
        }

        await s3.putObject({
            Bucket,
            Key,
            Body: newData,
            ContentType: 'application/json'
        }).promise();
        console.log(`ok - station: ${providerStation}`);
    }

    /**
     * Given a measures object, save it to s3
     *
     * @param {string} provider The name of the provider (ie purpleair)
     * @param {Measures} measures A measurements object of measures
     */
    static async put_measures(provider, measures) {
        const Bucket = process.env.BUCKET;
        const Key = `${process.env.STACK}/measures/${provider}/${Math.floor(Date.now() / 1000)}.csv`;
        return s3.putObject({
            Bucket,
            Key,
            Body: measures.csv(),
            ContentType: 'text/csv'
        }).promise();
    }
}

module.exports = Providers;
