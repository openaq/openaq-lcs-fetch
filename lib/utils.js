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

async function getSupportedLookups(lookup) {
    // https://0jac6b9iac.execute-api.us-east-1.amazonaws.com/v2/parameters?limit=100
    const supported_measurand_parameters = await Promise.resolve([
        'pm10',
        'pm25',
        'o3',
        'co',
        'no2',
        'so2',
        'no2',
        'co',
        'so2',
        'o3',
        'bc',
        'co2',
        'pm1',
        'co2',
        'nox',
        'nox',
        'ch4',
        'ufp',
        'no',
        'pm',
        'um003',
        'um010',
        'um050',
        'um025',
        'pm100',
        'um005',
        'um100',
        'voc',
        'nox',
        'bc'
    ]);

    const supportedLookups = Object.entries(lookup).filter(
        // eslint-disable-next-line no-unused-vars
        ([inputParam, [measurand_parameter, measurand_unit]]) => supported_measurand_parameters.includes(measurand_parameter)
    );
    if (!supportedLookups.length) throw new Error('No measurands supported.')
    if (VERBOSE && Object.keys(lookup).length - supportedLookups.length) {
        const missingParameters = Object.values(lookup)
            .map(([measurand_parameter]) => measurand_parameter)
            .filter((measurand_parameter) => !supported_measurand_parameters.includes(measurand_parameter));
        console.debug(`warning - ignoring ${Object.keys(lookup).length - supportedLookups.length} parameters: ${missingParameters.join(', ')}`);
    }
    return supportedLookups.map(
        ([input_param, [measurand_parameter, measurand_unit]]) => ({ input_param, measurand_parameter, measurand_unit })
    );
}

module.exports = {
    fetchSecret,
    measurandNormalizer,
    getSupportedLookups,
    VERBOSE
};
