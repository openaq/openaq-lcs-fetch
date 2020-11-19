'use strict';

const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const s3 = new AWS.S3({
    maxRetries: 10
});

/**
 * @class Providers
 */
class Providers {
    constructor() {
        for (const file of fs.readdirSync(path.resolve(__dirname, './providers'))) {
            this[path.parse(file).name] = require('./providers/' + file);
        }
    }

    /**
     * @param {String} source_name
     * @param {Object} source
     */
    async processor(source_name, source) {
        if (!this[source.provider]) throw new Error(`${source.provider} is not a supported provider`);

        try {
            await this[source.provider].processor(source_name, source);
        } catch (err) {
            throw err;
        }
    }

    static async put_stations(provider, stations) {
        for (const sta of stations) {
            await this.put_station(provider, sta)
        }

        return true;
    }

    static async put_station(provider, station) {
        return new Promise((resolve, reject) => {
            const key = `${provider}/${station.sensor_node_id}`;

            s3.putObject({
                Body: JSON.stringify(station.json()),
                Bucket: process.env.BUCKET,
                Key: `${process.env.STACK}/stations/${key}`
            }, (err, res) => {
                if (err) return reject(err);
                console.log(`ok - stations: ${key}`);

                return resolve(res);
            });
        });
    }

    /**
     * Given a measures object, save it to s3
     * @param {string} provider The name of the provider (ie purpleair)
     * @param {Measures} measures A measurements object of measures
     */
    static async put_measures(provider, measures) {
        return new Promise((resolve, reject) => {
            s3.putObject({
                Body: measures.csv(),
                Bucket: process.env.BUCKET,
                Key: `${process.env.STACK}/measures/${provider}/${Math.floor(Date.now() / 1000)}.csv`
            }, (err, res) => {
                if (err) return reject(err);

                return resolve(res);
            });
        });
    }
}

module.exports = Providers;
