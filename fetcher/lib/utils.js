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
 * Transform phrase to camel case.
 * e.g. toCamelCase("API Key") === "apiKey"
 * 
 * @param {string} phrase 
 * @returns {string}
 */
function toCamelCase(phrase) {
    return phrase
        .split(' ')
        .map(word => word.toLowerCase())
        .map((word, i) => i === 0 ? word : word.replace(/^./, word[0].toUpperCase()))
        .join('')
}

module.exports = {
    fetchSecret,
    request,
    toCamelCase,
    VERBOSE
};
