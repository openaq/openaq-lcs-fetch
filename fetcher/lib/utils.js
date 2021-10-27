const { promisify } = require('util');
const request = promisify(require('request'));
const AWS = require('aws-sdk');

const VERBOSE = !!process.env.VERBOSE;

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

    const SecretId = `${process.env.SECRET_STACK || process.env.STACK}/${source_name}`;

    if (VERBOSE) console.debug(`Fetching ${SecretId}...`);

    const { SecretString } = await secretsManager.getSecretValue({
        SecretId
    }).promise();

    return JSON.parse(SecretString);
}

/**
 * Method to return a standardized date string
 *
 * @returns {string}
 */
function currentDateString() {
  const dt = new Date();
  const month = ("0" + dt.getMonth()).slice(-2);
  const day = ("0" + dt.getDate() + 1).slice(-2);
  const year = dt.getFullYear();
  return `${year}${month}${day}`;
}

/**
 * Method to return a standardized datetime string.
 *
 * @returns {string}
 */
function currentDatetimeString() {
  const dt = new Date();
  const month = ("0" + dt.getMonth()).slice(-2);
  const day = ("0" + dt.getDate() + 1).slice(-2);
  const year = dt.getFullYear();
  const hours = dt.getHours();
  const minutes = dt.getMinutes();
  const seconds = dt.getSeconds();
  return `${year}${month}${day}${hours}${minutes}${seconds}`;
}

module.exports = {
    fetchSecret,
    request,
    currentDateString,
    currentDatetimeString,
    VERBOSE,
};
