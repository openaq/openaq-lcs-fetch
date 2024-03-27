const fs = require('fs');
const path = require('path');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const {
    VERBOSE,
    DRYRUN,
    gzip,
    fetchSecret,
    getObject,
    putObject,
    prettyPrintStation
} = require('./utils');


const sns = new SNSClient();

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
     * @param {Object} source
     */
    async processor(source) {
        if (VERBOSE) console.debug('Processing', source.provider);
        if (!this[source.provider]) throw new Error(`${source.provider} is not a supported provider`);
        // fetch any secrets we may be storing for the provider
        if (VERBOSE) console.debug('Fetching secret: ', source.secretKey);
        const config = await fetchSecret(source);
        // and combine them with the source config for more generic access
        if (VERBOSE) console.log('Starting processor', { ...source, ...config });
        const log = await this[source.provider].processor({ ...source, ...config });
        // source_name is more consistent with our db schema
        if (typeof(log) == 'object' && !Array.isArray(log) && !log.source_name) {
            log.source_name = source.provider;
        }
        return (log);
    }

    /**
     * Publish the results of the fetch to our SNS topic
     *
     * @param {Object} message
     * @param {String} subject
     */
    async publish(message, subject) {
        console.log('Publishing:', subject, message);
        if (process.env.TOPIC_ARN && message) {
            const cmd = new PublishCommand({
                TopicArn: process.env.TOPIC_ARN,
                Subject: subject,
                Message: JSON.stringify(message)
            });
            return await sns.send(cmd);
        } else {
            return {};
        }
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
            const currentData = await getObject(Bucket, Key);
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
            if (err.Code !== 'NoSuchKey') throw err;
        }

        const compressedString = await gzip(newData);

        if (!DRYRUN) {
            if (VERBOSE) console.debug(`Saving station to ${Bucket}/${Key}`);
            await putObject(
                compressedString,
                Bucket,
                Key,
                false,
                'application/json',
                'gzip'
            );
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

        return await putObject(compressedString, Bucket, Key, false, 'text/csv', 'gzip');
    }
}

module.exports = Providers;
