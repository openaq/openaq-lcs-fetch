'use strict';

const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();

class Providers {
    constructor() {
        for (const file of fs.readdirSync(path.resolve(__dirname, './providers'))) {
            this[path.parse(file).name] = require('./providers/' + file);
        }
    }

    async process(source_name, source) {
        if (!this[source.provider]) throw new Error(`${source.provider} is not a supported provider`);

        try {
            await this[source.provider].process(source_name, source);
        } catch (err) {
            throw new Error(err);
        }
    }

    static async put_station(provider, station) {
        return new Promise((resolve, reject) => {
            s3.putObject({
                Body: station.json(),
                Bucket: process.env.BUCKET,
                Key: `${process.env.STACK}/stations/${provider}/${station.id}`
            }, (err, res) => {
                if (err) return reject(err);

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
