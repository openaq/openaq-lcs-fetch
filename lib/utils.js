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
    const supported_measurand_parameters = [];
    let morePages;
    let page = 1;
    do {
        const url = new URL('/v2/parameters', process.env.LCS_API);
        url.searchParams.append('page', page++);
        const { body: { meta, results } } = await request({
            json: true,
            method: 'GET',
            url
        });
        for (const { name } of results) {
            supported_measurand_parameters.push(name);
        }
        morePages = meta.found > meta.page * meta.limit;
    } while (morePages);
    if (VERBOSE) console.debug(`Fetched ${supported_measurand_parameters.length} supported measurement parameters.`)

    const supportedLookups = Object.entries(lookup).filter(
        // eslint-disable-next-line no-unused-vars
        ([input_param, [measurand_parameter, measurand_unit]]) => supported_measurand_parameters.includes(measurand_parameter)
    );
    if (!supportedLookups.length) throw new Error('No measurands supported.');
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
