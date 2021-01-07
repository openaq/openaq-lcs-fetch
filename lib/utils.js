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
 * Normalize unit and values of a given measurand.
 * @param {string} measurand_unit
 *
 * @returns { Object } normalizer
 */
function measurandNormalizer(measurand_unit) {
    const lookup = {
        ppb: ['ppm', (val) => (val / 1000)],
        'ng/m³': ['µg/m³', (val) => (val / 1000)]
    };
    const noop = [measurand_unit, (val) => val];
    const [unit, value] = lookup[measurand_unit] || noop;
    return { unit, value };
}


module.exports = {
    fetchSecret,
    measurandNormalizer,
    VERBOSE
};
