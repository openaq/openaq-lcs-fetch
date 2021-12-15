const { request, VERBOSE } = require('./utils');

class Measurand {
    constructor({ input_param, parameter, unit }) {
        // How a measurand is described by external source (e.g. "CO")
        this.input_param = input_param;
        // How a measurand is described internally (e.g. "co")
        this.parameter = parameter;
        // Unit for measurand (e.g "ppb")
        this.unit = unit;
    }

    /**
     * Normalize unit and values of a given measurand.
     * @param {string} unit
     *
     * @returns { Object } normalizer
     */
    get _normalizer() {
        return {
            ppb: ['ppm', (val) => (val / 1000)],
            'ng/m³': ['µg/m³', (val) => (val / 1000)],
            'pp100ml': ['particles/cm³', (val) => (val / 100)]
        }[this.unit] || [this.unit, (val) => val];
    }

    get normalized_unit() {
        return this._normalizer[0];
    }

    get normalize_value() {
        return this._normalizer[1];
    }

    /**
     * Given a map of lookups from an input parameter (i.e. how a data provider
     * identifies a measurand) to a tuple of a measurand parameter (i.e. how we
     * idenify a measurand internally) and a measurand unit, generate an array
     * Measurand objects that are supported by the OpenAQ API.
     *
     * @param {*} lookups, e.g. {'CO': ['co', 'ppb'] }
     * @returns { Measurand[] }
     */
    static async getSupportedMeasurands(lookups) {
      // Fetch from API
        const supportedMeasurandParameters = [];
        let morePages;
        let page = 1;
        do {
            const url = new URL('/v2/parameters', process.env.API_URL || 'https://api.openaq.org');
            url.searchParams.append('page', page++);
            const { body: { meta, results } } = await request({
                json: true,
                method: 'GET',
                url
            });
            for (const { name } of results) {
                supportedMeasurandParameters.push(name);
            }
            morePages = meta.found > meta.page * meta.limit;
        } while (morePages);
      //if (VERBOSE)
        console.debug(`Fetched ${supportedMeasurandParameters.length} supported measurement parameters.`);

        // Filter provided lookups
        const supportedLookups = Object.entries(lookups).filter(
            // eslint-disable-next-line no-unused-vars
            ([input_param, [measurand_parameter, measurand_unit]]) => supportedMeasurandParameters.includes(measurand_parameter)
        );

        if (!supportedLookups.length) throw new Error('No measurands supported.');
        if (VERBOSE) {
            Object.values(lookups)
                .map(([measurand_parameter]) => measurand_parameter)
                .filter((measurand_parameter) => !supportedMeasurandParameters.includes(measurand_parameter))
                .map((measurand_parameter) => console.debug(`warning - ignoring unsupported parameters: ${measurand_parameter}`));
        }

        return supportedLookups.map(
            ([input_param, [parameter, unit]]) => (
                new Measurand({ input_param, parameter, unit })
            )
        );
    }
}

module.exports = { Measurand };
