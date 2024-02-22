const { request, VERBOSE } = require('./utils');

class Measurand {
    constructor({ input_param, parameter, unit, provider_unit }) {
        // How a measurand is described by external source (e.g. "CO")
        this.input_param = input_param;
        // How a measurand is described internally (e.g. "co")
        this.parameter = parameter;
        // Unit for measurand (e.g "ppb")
        this.unit = unit;
				this.provider_unit = provider_unit
    }

    /**
     * Normalize unit and values of a given measurand.
     * @param {string} unit
     *
     * @returns { Object } normalizer
     */
    get _normalizer() {
				// provider_units: { unit: conversion function }
        return ({
                ppb: {
										ppm: (val) => val / 1000
								},
                ppm: {
										ppb: (val) => val * 1000
								},
								f: {
										c: (val) => (val - 32) * 5/9
								},
                'ng/m3': {
										'ug/m3': (val) => val / 1000
								},
                pp100ml: {
										'particles/cm³': (val) => val / 100
								},
						}[this.provider_unit][this.unit]) ?? ((val) => val);
        ;
    }

    get normalized_unit() {
				return this.unit;
    }

    get normalize_value() {
        return this._normalizer
    }

    /**
     * Given a map of lookups from an input parameter (i.e. how a data provider
     * identifies a measurand) to a tuple of a measurand parameter (i.e. how we
     * identify a measurand internally) and a measurand unit, generate an array
     * Measurand objects that are supported by the OpenAQ API.
     * form -> { input_parameter : [ measurand_parameter, input_units ] }
		 *
     * @param {*} lookups, e.g. {'CO': ['co', 'ppb'] }
     * @returns { Measurand[] }
     */
    static async getSupportedMeasurands(lookups) {
				// we are supporting everything in the fetcher
				const supportedMeasurandParameters = {
						ambient_temp: 'c',
						bc: 'ug/m3',
						bc_375: 'ug/m3',
						bc_470: 'ug/m3',
						bc_528: 'ug/m3',
						bc_625: 'ug/m3',
						bc_880: 'ug/m3',
						ch4: 'ppm',
						cl: 'ppb',
						co2: 'ppm',
						co: 'ppm',
						ec: 'ppb',
						humidity: '%',
						no2: 'ppm',
						no3: 'ppb',
						no: 'ppm',
						nox: 'ppm',
						o3: 'ppm',
						oc: 'ppb',
						ozone: 'ppb',
						pm100: 'ug/m3',
						pm10: 'ug/m3',
						pm1: 'ug/m3',
						pm25: 'ug/m3',
						pm4: 'ug/m3',
						pm: 'ug/m3',
						pressure: 'hpa',
						relativehumidity: '%',
						so2: 'ppm',
						so4: 'ppb',
						temperature: 'c',
						ufp: 'particles/cm3',
						um003: 'particles/cm3',
						um005: 'particles/cm3',
						um010: 'particles/cm3',
						um025: 'particles/cm3',
						um050: 'particles/cm3',
						um100: 'particles/cm3',
						v: 'ppb',
						voc: 'iaq',
						wind_direction: 'deg',
						wind_speed: 'm/s',
				};

        let supported = Object.entries(lookups)
						.map(([input_param, [parameter, provider_unit]]) => {
								return new Measurand({
										input_param,
										parameter,
										unit: supportedMeasurandParameters[parameter],
										provider_unit
								})
						}).filter( m => m.unit)
          //  ([input_param, [parameter, unit, provider_unit]]) =>
          //      new Measurand({ input_param, parameter, unit, provider_unit })
				if (VERBOSE>1) console.log('Supported measurands', supported)
				return supported
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
