const tape = require('tape');

const generic = require('../fetcher/providers/generic.js');

const config = {
    provider: 'testing',
    meta: {
      "parameter_key": "parameter",
      "value_key": "value"
    },
    parameters: {
    "co": ["co", "ppb"],
    "wd": ["wind_direction", "deg"],
    "ws": ["wind_speed", "m/s"]
    }
};

tape('wide measurements work', async (t) => {

    const client = new generic.Client(config);
    await client.fetchMeasurands();

    const files = [
        {
            "type": "measurements",
            "path": `${__dirname}/test_measurements_wide.csv`
        }
    ];

    await Promise.all(files.map(async (file) => {
        await client.processData(file);
    }));

    const data = client.data();

    t.equal(data.meta.source, 'testing', 'has correct source name');
    t.equal(data.locations.length, 0, 'no locations were added');
    t.equal(data.measures.length, 2, 'two measurements were added');
    t.equal(data.measures[0].sensor_id, 'testing-test_site_1-co', 'has correct ingest id');

    t.end();
});


tape('long measurements work', async (t) => {

    const client = new generic.Client(config);
    await client.fetchMeasurands();

    const files = [
        {
            "type": "measurements",
            "path": `${__dirname}/test_measurements_long.csv`
        }
    ];

    await Promise.all(files.map(async (file) => {
        await client.processData(file);
    }));

    const data = client.data();

    t.equal(data.meta.source, 'testing', 'has correct source name');
    t.equal(data.locations.length, 0, 'no locations were added');
    t.equal(data.measures.length, 2, 'two measurements were added');
    t.equal(data.measures[0].sensor_id, 'testing-test_site_1-co', 'has correct ingest id');

    t.end();
});

tape('simple locations work', async (t) => {

    const client = new generic.Client(config);
    await client.fetchMeasurands();

    const files = [
        {
            "type": "locations",
            "path": `${__dirname}/test_locations.csv`
        }
    ];

    await Promise.all(files.map(async (file) => {
        await client.processData(file);
    }));

    const data = client.data();

    t.equal(data.meta.source, 'testing', 'meta contains the right source name');
    t.equal(data.locations.length, 2, 'locations were added');
    t.equal(data.measures.length, 0, 'no measurements were added');
    t.equal(data.locations[0].systems.length, 0, 'no systems were added');
    t.equal(!!data.locations[0].lat, true, 'first location has a latitude');
    t.equal(!!data.locations[0].lon, true, 'first location has a longitude');
    t.equal(data.locations[0].location, 'testing-test_site_1', 'first location has correct location id');

    t.end();
});


tape('advanced locations work', async (t) => {

    const client = new generic.Client(config);
    await client.fetchMeasurands();

    const files = [
        {
            "type": "locations",
            "path": `${__dirname}/test_advanced_locations.csv`
        }
    ];

    await Promise.all(files.map(async (file) => {
        await client.processData(file);
    }));

    const data = client.data();
    const loc = data.locations[0];

    t.equal(loc.location, 'testing-test_site_1', 'has correct location id');
    t.equal(Object.keys(loc.metadata).includes('project'), true, 'project exists in metadata');
    t.equal(Object.keys(loc.metadata).includes('city'), true, 'city exists in metadata');
    t.equal(Object.keys(loc.metadata).includes('state'), true, 'state exists in metadata');
    t.equal(Object.keys(loc.metadata).includes('country'), true, 'country exists in metadata');

    t.end();
});

tape('simple simple sensors work', async (t) => {

    const client = new generic.Client(config);
    await client.fetchMeasurands();

    const files = [
        {
            "type": "sensors",
            "path": `${__dirname}/test_sensors_simple.csv`
        }
    ];

    await Promise.all(files.map(async (file) => {
        await client.processData(file);
    }));

    const data = client.data();
    const loc1 = data.locations[0];
    const loc2 = data.locations[1];

    t.equal(data.locations.length, 2, 'locations were added');
    t.equal(loc1.systems.length, 2, 'Two different systems were added to #1');
    t.equal(loc1.systems[0].sensors.length, 1, '1 sensor added to system');
    t.equal(loc2.systems.length, 1, 'One systems were added to #2');
    t.equal(loc2.systems[0].sensors.length, 2, '2 sensors added to system');
    t.equal(loc2.systems[0].sensors[0].status, 'u', 'has correct status');
    t.equal(loc2.systems[0].system_id, 'testing-test_site_2-metone:aio2', 'Has correct system name');
    t.end();
});


tape('simple all files work', async (t) => {

    const client = new generic.Client(config);
    await client.fetchMeasurands();

    const files = [
        {
            "type": "locations",
            "path": `${__dirname}/test_locations.csv`
        },
        {
            "type": "sensors",
            "path": `${__dirname}/test_sensors_simple.csv`
        },
        {
            "type": "measurements",
            "path": `${__dirname}/test_measurements_wide.csv`
        }
    ];

    await Promise.all(files.map(async (file) => {
        await client.processData(file);
    }));

    const data = client.data();
    const loc1 = data.locations[0];
    const loc2 = data.locations[1];

    t.equal(data.meta.source, 'testing', 'has correct source name');
    t.equal(data.measures.length, 2, 'two measurements were added');
    t.equal(data.measures[0].sensor_id, 'testing-test_site_1-co', 'has correct ingest id');
    t.equal(data.locations.length, 2, 'locations were added');
    t.equal(loc1.systems.length, 2, 'Two different systems were added to #1');
    t.equal(loc1.systems[0].sensors.length, 1, '1 sensor added to system');
    t.equal(loc2.systems.length, 1, 'One systems were added to #2');
    t.equal(loc2.systems[0].sensors.length, 2, '2 sensors added to system');
    t.equal(loc2.systems[0].system_id, 'testing-test_site_2-metone:aio2', 'Has correct system name');
    t.end();
});


tape('versioned sensors work', async (t) => {

    const client = new generic.Client(config);
    await client.fetchMeasurands();

    const files = [
        {
            "type": "sensors",
            "path": `${__dirname}/test_sensors_versioned.csv`
        }
    ];

    await Promise.all(files.map(async (file) => {
        await client.processData(file);
    }));

    const data = client.data();
    const loc1 = data.locations[0];

    t.equal(data.locations.length, 1, 'locations were added');
    t.equal(loc1.systems.length, 1, 'systems were added to #1');
    t.equal(loc1.systems[0].sensors.length, 2, '2 sensor added to system');
    t.end();
});


/**
 * This would be the case when one node/location has more than one
 * instance of a given parameter/measurand
 */
tape('sensors instances work', async (t) => {

    const client = new generic.Client(config);
    await client.fetchMeasurands();

    const files = [
        {
            "type": "sensors",
            "path": `${__dirname}/test_sensor_instances.csv`
        }
    ];

    await Promise.all(files.map(async (file) => {
        await client.processData(file);
    }));

    const data = client.data();
    const loc1 = data.locations[0];

    t.equal(data.locations.length, 1, 'locations were added');
    t.equal(loc1.systems.length, 1, 'systems were added to #1');
    t.equal(loc1.systems[0].sensors.length, 2, '2 sensor added to system');
    t.end();
});
