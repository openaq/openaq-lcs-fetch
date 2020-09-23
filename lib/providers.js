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

    async process(source) {
        if (!this[source.provider]) throw new Error(`${source.provider} is not a supported provider`);

        try {
            await this[source.provider].process(source);
        } catch (err) {
            throw new Error(err);
        }
    }

    static async put_station(station) {
        return new Promise((resolve, reject) => {
            s3.putObject({
                Body: JSON.stringify(station),
                Bucket: '',
                Key
            }, (err, res) => {
                if (err) return reject(err);

                return resolve(res);
            });
        });
    }

    static async put_measures(measures) {
        return new Promise((resolve, reject) => {
            s3.putObject({
                Body: measures.join('\n'),
                Bucket: '',
                Key
            }, (err, res) => {
                if (err) return reject(err);

                return resolve(res);
            });
        });
    }
}

module.exports = Providers;
