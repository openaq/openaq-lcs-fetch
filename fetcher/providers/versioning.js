const csv = require('csv-parser');
const pLimit = require('p-limit');
const Providers = require('../lib/providers');
const path = require('path');
const { promisify } = require('util');
const zlib = require('zlib');
const fs = require('fs');
const readdir = promisify(fs.readdir);
const gzip = promisify(zlib.gzip);
const { currentDateString } = require('../lib/utils');
//const unzip = promisify(zlib.unzip);

const {
  Sensor,
  Version,
  SensorNode,
  SensorSystem,
} = require('../lib/station');

const {
  Measures,
  FixedMeasure
} = require('../lib/measure');

const {
  Measurand
} = require('../lib/measurand');

const {
  VERBOSE,
 // fetchSecret
} = require('../lib/utils');

// const {
//   MetaDetails
// } = require('../lib/meta');


/**
 * Add the station data to the local directory. Just for testing purposes
 *
 * @param {string} provider
 * @param {object} station
 *
 * @returns {string}
 */
const put_station = async (provider, station) => {
  // simply write the staion to a local folder

  const providerStation = `${provider}-${station.sensor_node_id}.json.gz`;
  const filePath = path.join(__dirname, '../data/cmu/stations', providerStation);
  const compressedString = await gzip(JSON.stringify(station.json()));
  await fs.promises.writeFile(filePath, compressedString);
  return(filePath);
};

/**
 * Add the measurement data to the local directory. Just for testing purposes.
 *
 * @param {string} provider
 * @param {object} station
 *
 * @returns {string}
 */
const put_measures = async (provider, measures, id) => {
  // simply write the measurements to the local folder
  if (!measures.length) {
    return console.warn('No measures found, not adding to local folder.');
  }
  const filename = `${id}.csv.gz`;
  const filePath = path.join(__dirname, '../data/cmu/measurements', filename);
  const compressedString = await gzip(measures.csv());
  if(VERBOSE) {
    console.log('Adding measurements to local directory', id, filename);
  }
  await fs.promises.writeFile(filePath, compressedString);
  return(filePath);
};


/**
 * Add the measurement data to the local directory. Just for testing purposes.
 *
 * @param {string} provider
 * @param {object} station
 *
 * @returns {string}
 */
const put_version = async (location, parameter, lifecycle, isNew) => {
  const filename = `${id}.csv.gz`;
  const filePath = path.join(__dirname, '../data/cmu/measurements', filename);
  const compressedString = await gzip(measures.csv());
  if(VERBOSE) {
    console.log('Adding measurements to local directory', id, filename);
  }
  await fs.promises.writeFile(filePath, compressedString);
  return(filePath);
};



/**
 * Build sensor ID from a given sensor node ID and measurand parameter.
 *
 * @param {string} sourceId
 * @param {string} sensorNodeId
 * @param {string} measurandParameter
 *
 * @returns {string}
 */
function getRootSensorId(sourceId, sensorNodeId, measurandParameter) {
  return `${sourceId}-${sensorNodeId}-${measurandParameter}`;
}


/**
 * Build sensor ID from a given sensor node ID and measurand parameter.
 *
 * @param {string} sourceId
 * @param {string} sensorNodeId
 * @param {string} measurandParameter
 * @param {string} lifeCycle
 * @param {boolean} isVersion
 *
 * @returns {string}
 */
function getSensorId(sourceId, sensorNodeId, measurandParameter, lifeCycle, isVersion) {
  const lifeCycleId = !!lifeCycle
        ? `-${lifeCycle}`
        : '';
  // if no lifecyle value is provided we should assume this is raw data
  const versionId = isVersion && !!lifeCycle
        ? `-${currentDateString()}`
        : '';
  return `${sourceId}-${sensorNodeId}-${measurandParameter}${lifeCycleId}${versionId}`;
}

/**
 * Fetch and then process files for ingest
 *
 * @param {string} source_name - The name used for the source
 * @param {object} source - An object that also includes the source name?
 *
 * @returns {string}
 */
async function processor(source_name, source) {

  const { directory, parameters } = source.meta;
  const limit = pLimit(10);
  const directoryPath = path.join(__dirname, directory, 'staging');
  const filesToProcess = [];
  const stations = {};

  const [
    measurands,
    //credentials
  ] = await Promise.all([
    Measurand.getSupportedMeasurands(parameters),
    //fetchSecret(source.provider)
  ]);

  // First we need to get the list of files to process
  const files = await readdir(directoryPath);

  // Next we are going to loop through them and
  // read them in as a json array
  await Promise.all(files.map( async (filename) => {
    let filepath = path.resolve(directoryPath, filename);
    let data = [];
    let file = {
      name: filename,
      path: filepath,
    };
    return new Promise((resolve, reject) => {
      fs.createReadStream(filepath)
        .pipe(csv())
        .on('data', row => data.push(row))
        .on('end', () => {
          // trying to remain consistent with other providers
          // so we are using the limit method
          filesToProcess.push(
            limit(() => process({ file, data, stations, measurands }))
          );
          resolve();
        });
    });
  }));

  // what is the point of this?
  await Promise.all(Object.values(stations));
  console.log(`ok - all ${Object.values(stations).length} fixed stations pushed`);

  await Promise.all(filesToProcess);
  console.log(`ok - all ${filesToProcess.length} files processed`);

}

/**
 * Return a query for years and months (represented as a string, formatted as
    such: 'YYYY-MM') that occurred between two times, inclusive
 *
 * @param {object} file - file information to use for saving the final csv file
 * @param {array} data - the data, in wide format, to be processed
 * @param {object} stations - an empty object to store station data (??)
 * @param {Measurands} measurands - list of the proper measurand names
 *
 * @returns {??}
 */
async function process({ file, data, stations, measurands }) {

  const measures = new Measures(FixedMeasure);
  const sourceId = 'versioning';

  for(const row of data) {
    let sensorNodeId = row.location;
    let { lifecycle, version } = row;
    // Compile the station information and check if it exists
    // If not this will add the sensor node to the stations directory
    // this will trigger an injest of the station data
    if(!stations[sensorNodeId]) {
      stations[sensorNodeId] = put_station(
        sourceId,
        new SensorNode({
          sensor_node_id: sensorNodeId,
          sensor_node_site_name: sensorNodeId,
          //sensor_node_geometry: [Lon, Lat],
          sensor_node_source_name: sourceId,
          sensor_node_ismobile: false,
          sensor_system: new SensorSystem({
            sensors: measurands
              .map((measurand) => (
                new Sensor({
                  sensor_id: getSensorId(sourceId, sensorNodeId, measurand.parameter, lifecycle, version),
                  measurand_parameter: measurand.parameter,
                  measurand_unit: measurand.normalized_unit
                })
              ))
          })
        })
      );
    }
    // Compile the version information and check if it exists
    // if not the version will be added to the versions directory
    // and trigger an import


    // process that measurements to a standard (long) format
    for (const measurand of measurands) {
      const measure = row[measurand.input_param];
      if ([undefined, null, 'NaN'].includes(measure)) continue;
      measures.push({
        sensor_id: getSensorId(sourceId, sensorNodeId, measurand.parameter, lifecycle, version),
        measure: measurand.normalize_value(measure),
        timestamp: row.datetime,
      });
    }
  }

  // Now
  const filename = file.name.endsWith('.csv') ? file.name.slice(0, -4) : file.name;
  return put_measures(sourceId, measures, filename);
}


module.exports = {
    processor,
};
