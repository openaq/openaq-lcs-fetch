'use strict';

const { promisify } = require('util');
const request = promisify(require('request'));
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');
const {google} = require('googleapis');

async function processor(source_name, source) {
    if (!process.env.SecretPurpleAir) throw new Error('SecretPurpleAir Env Var Required');

    const stas = [];

    try {

        const drive = google.drive({ version: 'v3' });


        console.log(`ok - pulled ${locs.length} stations`);

        for (const loc of locs) {
            const system = new SensorSystem();

            const sta = new SensorNode({
                sensor_node_id: loc.sensor_index,
                sensor_node_source_name: 'PurpleAir',
                sensor_node_site_name: loc.location_type,
                sensor_node_geometry: [ loc.longitude, loc.latitude ],
                sensor_node_ismobile: false,
                sensor_system: system
            });

            for (const cnt of Object.keys(lookup)) {
                if (loc[cnt] === null) continue;

                const sensor = new Sensor({
                    sensor_id: `${sta.sensor_node_source_name}-${loc.sensor_index}-${cnt}`,
                    measurand_parameter: lookup[cnt][0],
                    measurand_unit: lookup[cnt][1]
                });

                system.sensors.push(sensor);
            }

            stas.push(Providers.put_station(source_name, sta));
        }

        await Promise.all(stas);
        console.log('ok - all stations pushed');

        const measures = [`"sensor_id","measure"`];
        locs.forEach((loc) => {
            for (const m of Object.keys(lookup)) {
                measures.push(`"PurpleAir-${loc.sensor_index}-${m}","${loc[m]}"`);
            }
        });

        await Providers.put_measures(source_name, measures);
    } catch (err) {
        throw new Error(err);
    }
}

module.exports = {
    processor
};
