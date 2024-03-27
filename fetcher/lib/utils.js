const zlib = require('zlib');
const { promisify } = require('util');
const request = promisify(require('request'));

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const VERBOSE = !!process.env.VERBOSE;
const DRYRUN = !!process.env.DRYRUN;

const s3 = new S3Client({
    maxRetries: 10
});

const gzip = promisify(zlib.gzip);
const unzip = promisify(zlib.unzip);


/**
 * Returns the data from an s3 file
 * @param {string} Bucket name of the s3 bucket
 * @param {string} Key - s3 key
 * @returns {Object} data in the file
 */
async function getObject(Bucket, Key) {
    const cmd = new GetObjectCommand({
        Bucket,
        Key
    });
    const resp = await s3.send(cmd);
    let currentData = null;
    if (resp && resp.ContentEncoding === 'gzip') {
        const ba = await resp.Body.transformToByteArray();
        currentData = (await unzip(Buffer.from(ba))).toString('utf-8');
    } else if (resp && resp.Body) {
        currentData = await resp.Body.transformToString();
    }
    return currentData;
}

/**
 *  New method to put a file in the s3 bucket
 * @param {string} text data that will go in the file
 * @param {string} Bucket name of the s3 bucket
 * @param {string} Key s3 file path
 * @param {boolean} gzip should we gzip the data
 * @param {string} ContentType content type header
 * @param {string} ContentEncoding
 * @returns {object} response from aws
 */
async function putObject(text, Bucket, Key, gzip = true, ContentType = 'application/json', ContentEncoding = null) {
    if (gzip) {
        text = await gzip(text);
        ContentEncoding = 'gzip';
    }
    const cmd = new PutObjectCommand({
        Bucket,
        Key,
        Body: text,
        ContentType,
        ContentEncoding
    });
    return await s3.send(cmd);
}

/**
 * Retrieve secret from AWS Secrets Manager
 * @param {string} source The source object for which we are fetching a secret.
 *
 * @returns {object}
 */
async function fetchSecret(source) {
    const key = source.secretKey;
    if (!key) {
        return {};
    }
    const secretsManager = new SecretsManagerClient({
        region: process.env.AWS_DEFAULT_REGION || 'us-east-1',
        maxAttemps: 1
    });

    if (!process.env.STACK) throw new Error('STACK Env Var Required');

    const SecretId = `${
        process.env.SECRET_STACK || process.env.STACK
    }/${key}`;

    if (VERBOSE) console.debug(`Fetching ${SecretId} secret...`);

    const cmd = new GetSecretValueCommand({
        SecretId
    });

    const resp = await secretsManager
        .send(cmd)
        .catch((err) => console.error(`Missing ${key} secret: ${err}`));
    if (resp && resp.SecretString) {
        return JSON.parse(resp.SecretString);
    } else {
        return {};
    }
}

/**
 * Transform phrase to camel case.
 * e.g. toCamelCase("API Key") === "apiKey"
 *
 * @param {string} phrase
 * @returns {string}
 */
function toCamelCase(phrase) {
    return phrase
        .split(' ')
        .map((word) => word.toLowerCase())
        .map((word, i) => {
            if (i === 0) return word;
            return word.replace(/^./, word[0].toUpperCase());
        })
        .join('');
}

/**
 * Print out JSON station object
 * @param {obj} station
 */
function prettyPrintStation(station) {
    if (typeof station === 'string') {
        station = JSON.parse(station);
    }
    for (const [key, value] of Object.entries(station)) {
        if (key !== 'sensor_systems') {
            console.log(`${key}: ${value}`);
        } else {
            console.log('Sensor systems');
            value.map((ss) => {
                for (const [ky, vl] of Object.entries(ss)) {
                    if (ky !== 'sensors') {
                        console.log(`-- ${ky}: ${vl}`);
                    } else {
                        vl.map((s) =>
                            console.debug(
                                `---- ${s.sensor_id} - ${s.measurand_parameter} ${s.measurand_unit}`
                            )
                        );
                    }
                }
            });
        }
    }
}

/**
 * Make some simple, standard data checks before submitting the file
 * @param {array} data
 * @param {timestamp} start_timestamp
 * @param {timestamp} end_timestamp
 *
 * @returns {array}
 */
function checkResponseData(data, start_timestamp, end_timestamp) {
    const n = data && data.length;
    if (!n) return [];
    // no future data as default, obviously requres UTC
    if (!end_timestamp) {
        end_timestamp = Math.round(Date.now() / 1000);
    }
    // filter down to the requested period
    const fdata = data.filter((d) => {
        return (
            (d.time / 1000 >= start_timestamp || !start_timestamp) &&
      d.time / 1000 <= end_timestamp
        );
    });
    if (fdata.length < n) {
    // submit warning so we can track this
        const requested_start = new Date(
            start_timestamp * 1000
        ).toISOString();
        const returned_start = new Date(data[0].time).toISOString();
        const requested_end = new Date(
            end_timestamp * 1000
        ).toISOString();
        const returned_end = new Date(data[n - 1].time).toISOString();
        console.warn(
            `API returned more data than requested: requested: ${requested_start} > ${requested_end}, returned: ${returned_start} > ${returned_end}`
        );
    }
    return fdata;
}


module.exports = {
    fetchSecret,
    request,
    toCamelCase,
    gzip,
    unzip,
    VERBOSE,
    DRYRUN,
    getObject,
    putObject,
    prettyPrintStation,
    checkResponseData
};
