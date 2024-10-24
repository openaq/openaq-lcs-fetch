const { VERBOSE } = require('./utils');

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
        return (
            {
                ppb: ['ppm', (val) => val / 1000],
                'ng/m³': ['µg/m³', (val) => val / 1000],
                pp100ml: ['particles/cm³', (val) => val / 100],
                'pa': ['hPa', (val) => val / 100]
            }[this.unit] || [this.unit, (val) => val]
        );
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
     * identify a measurand internally) and a measurand unit, generate an array
     * Measurand objects that are supported by the OpenAQ API.
     *
     * @param {*} lookups, e.g. {'CO': ['co', 'ppb'] }
     * @returns { Measurand[] }
     */
    static async getSupportedMeasurands(lookups) {
        // Fetch from API
        const supportedMeasurandParameters = [
            'pm10',
            'pm25','o3','co','no2','so2','no2','co','so2','o3','bc','co2','no2','bc','pm1','co2','wind_direction','nox','no','rh','nox','ch4','pn','o3','ufp','wind_speed','no','pm','ambient_temp','pressure','pm25-old','relativehumidity','temperature','so2','co','um003','um010','temperature','um050','um025','pm100','pressure','um005','humidity','um100','voc','ozone','nox','bc','no','pm4','so4','ec','oc','cl','no3','pm25'];

        // Filter provided lookups
        const supportedLookups = Object.entries(lookups).filter(
            // eslint-disable-next-line no-unused-vars
            ([input_param, [measurand_parameter, measurand_unit]]) =>
                supportedMeasurandParameters.includes(measurand_parameter)
        );
        if (!supportedLookups.length) throw new Error('No measurands supported.');
        if (VERBOSE) {
            Object.values(lookups)
                .map(([measurand_parameter]) => measurand_parameter)
                .filter(
                    (measurand_parameter) =>
                        !supportedMeasurandParameters.includes(measurand_parameter)
                )
                .map((measurand_parameter) =>
                    console.debug(
                        `warning - ignoring unsupported parameters: ${measurand_parameter}`
                    )
                );
        }
        return supportedLookups.map(
            ([input_param, [parameter, unit]]) =>
                new Measurand({ input_param, parameter, unit })
        );
    }

    /**
     * Given a map of lookups from an input parameter (i.e. how a data provider
     * identifies a measurand) to a tuple of a measurand parameter (i.e. how we
     * identify a measurand internally) and a measurand unit, generate an object
     * of Measurand objects that are supported by the OpenAQ API, indexed by their
     * input parameter.
     *
     * @param {*} lookups  e.g. {'CO': ['co', 'ppb'] }
     * @returns {object}
     */
    static async getIndexedSupportedMeasurands(lookups) {
        const measurands = await Measurand.getSupportedMeasurands(lookups);
        return Object.assign(
            {},
            ...measurands.map((measurand) => ({ [measurand.input_param]: measurand }))
        );
    }
}

module.exports = { Measurand };
