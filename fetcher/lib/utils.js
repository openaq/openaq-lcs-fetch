const zlib = require('zlib');
const { promisify } = require('util');
const request = promisify(require('request'));
const AWS = require('aws-sdk');

const VERBOSE = !!process.env.VERBOSE;
const DRYRUN = !!process.env.DRYRUN;

/**
 * Retrieve secret from AWS Secrets Manager
 * @param {string} source_name The source for which we are fetching a secret.
 *
 * @returns {object}
 */
async function fetchSecret(source_name) {
    const secretsManager = new AWS.SecretsManager({
        region: process.env.AWS_DEFAULT_REGION || 'us-east-1'
    });

    if (!process.env.STACK) throw new Error('STACK Env Var Required');

    const SecretId = `${
        process.env.SECRET_STACK || process.env.STACK
    }/${source_name}`;

    if (VERBOSE) console.debug(`Fetching ${SecretId}...`);

    const { SecretString } = await secretsManager
        .getSecretValue({
            SecretId
        })
        .promise();

    return JSON.parse(SecretString);
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
    if (typeof(station) === 'string') {
        station = JSON.parse(station);
    }
    for (const [key, value] of Object.entries(station)) {
        if (key !== 'sensor_systems') {
            console.log(`${key}: ${value}`);
        } else {
            console.log('Sensor systems');
            value.map( (ss) => {
                for (const [ky, vl] of Object.entries(ss)) {
                    if (ky !== 'sensors') {
                        console.log(`-- ${ky}: ${vl}`);
                    } else {
                        vl.map((s) => console.debug(`---- ${s.sensor_id} - ${s.measurand_parameter} ${s.measurand_unit}`));
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
*/
function checkResponseData(data, start_timestamp, end_timestamp) {
  const n = data && data.length;
  if(!n) return [];
  // no future data as default, obviously requres UTC
  if(!end_timestamp) {
    end_timestamp = Math.round(Date.now()/1000);
  }
  // filter down to the requested period
  const fdata = data.filter(d => {
    return (d.time/1000 >= start_timestamp || !start_timestamp) && d.time/1000 <= end_timestamp;
  });
  if(fdata.length < n) {
    // submit warning so we can track this
    const requested_start = new Date(start_timestamp*1000).toISOString();
    const returned_start = new Date(data[0].time).toISOString();
    const requested_end = new Date(end_timestamp*1000).toISOString();
    const returned_end = new Date(data[n-1].time).toISOString();
    console.warn(`API returned more data than requested: requested: ${requested_start} > ${requested_end}, returned: ${returned_start} > ${returned_end}`);
  }
  return fdata;
}

const gzip = promisify(zlib.gzip);
const unzip = promisify(zlib.unzip);

module.exports = {
    fetchSecret,
    request,
    toCamelCase,
    gzip,
    unzip,
    VERBOSE,
    DRYRUN,
  prettyPrintStation,
  checkResponseData,
};
