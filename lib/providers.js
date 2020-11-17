'use strict';

const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const s3 = new AWS.S3({
    maxRetries: 10
});

class Providers {
    constructor() {
        for (const file of fs.readdirSync(path.resolve(__dirname, './providers'))) {
            this[path.parse(file).name] = require('./providers/' + file);
        }
    }

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
                console.error(`ok - stations: ${key}`);

                return resolve(res);
            });
        });
    }

    static async put_measures(provider, measures) {
        return new Promise((resolve, reject) => {
            s3.putObject({
                Body: measures.join('\n'),
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
