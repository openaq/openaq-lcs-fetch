/**
 * @fileOverview Adapter to process files within the versioning work flow.
 * This workflow is slightly different from the LCS workflow in that it allows (expects)
 * different file types (measurement, location and version). One of the challenges here is to
 * make sure that data from one file is not overwritten by data from a subsequent file.
 * For example, a measurement file is expected to provide the relationships between the
 * node, system and sensor information, but a location file would provide details about the
 * station but with no sensor information.
 * Methods in this script should save processed files in the following format
 * Locations: {source}-{location}.json.gz
 * Measurements: {filename}.gz
 * Versions: {source}-{parentSensorId}-{version}.json.gz
 * @name versioning.js
 * @author Christian Parker <chris@talloaks.io>
 * @license no license
 */
const csv = require('csv-parser');
const pLimit = require('p-limit');
const Providers = require('../lib/providers');
const path = require('path');
const { promisify } = require('util');
const zlib = require('zlib');
const fs = require('fs');
const readdir = promisify(fs.readdir);
const gzip = promisify(zlib.gzip);
const unzip = promisify(zlib.unzip);
const { currentDateString } = require('../lib/utils');
const AWS = require('aws-sdk');
//const unzip = promisify(zlib.unzip);

const readFile = promisify(fs.readFile);

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
 * Retrieve secret from AWS Secrets Manager
 * If we are working locally this will look to env for the values
 * @param {string} source_name The source for which we are fetching a secret.
 *
 * @returns {object}
 */
async function fetchSecret(source_name) {
  console.log('fetch secret', source_name);
    //return JSON.parse(SecretString);
}


const writeJson = async (data, filepath) => {
  const jsonString = path.extname(filepath) === ".gz"
        ? await gzip(JSON.stringify(data))
        : JSON.stringify(data);
  //await fs.promises.writeFile(filepath, jsonString);
  fs.writeFileSync(filepath, jsonString);
  return true;
};


const readJson = (filepath) => {
  var json = {};
  var buffer;
  if(fs.existsSync(filepath)) {
    //var buffer = await readFile(filepath);
    buffer = fs.readFileSync(filepath, 'utf-8');

    if(path.extname(filepath) == ".gz") {
      // decompress
    }
    if(buffer) {
      json = JSON.parse(buffer);
    }
  } else {
    console.log('file not found', filepath);
  }
  // as json
  return(json);
};




/**
 * Add the station data to the local directory. Just for testing purposes
 *
 * @param {string} provider
 * @param {object} station
 *
 * @returns {string}
 */
const put_station = async (sourceId, station) => {
  // simply write the staion to a local folder
  const providerStation = `${sourceId}-${station.sensor_node_id}.json`;
  const filepath = path.join(__dirname, '../data/cmu/stations', providerStation);

  const current = await get_station({ filepath });
  station.merge(current);

  await writeJson(station.json(), filepath);
  return(filepath);
};

const get_station = ({ filepath }) => {
  const data = readJson(filepath);
  return(data);
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
const put_version = async (version) => {
  //const basename = version.filename.slice(0, -4) + "-";
  const basename = "";
  const filename = `${basename}${version.sensor_id}.json`;
  const filepath = path.join(__dirname, '../data/cmu/versions', filename);
  const current = await get_version({ filepath });
  version.merge(current);
  //console.log(current, version.json());
  await writeJson(version.json(), filepath);
  return(filepath);
};


const get_version = ({ filepath }) => {
  const data = readJson(filepath);
  return(data);
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
 * Fetch files from the clients staging area and process them.
 * We are supporting 3 types of files for this adapter
 * A measurement file which follows the traditional format
 * A location file which provides metadata for the location/station
 * and a version file which allows for a readme to be added.
 *
 * @param {string} source_name - The name used for the source
 * @param {object} source - An object that also includes the source name?
 *
 * @returns {string}
 */
async function processor(source_name, source) {

  const { directory, parameters } = source.meta;
  const limit = pLimit(10);
  const directoryPath = path.join(__dirname, directory, 'staging/pending');
  const filesToProcess = [];
  // passing a stations object does not make sense when we have different file types
  // the concern is that a measurement file could create a station and then a new
  // location file would come along and the file would be ignored. If this is needed
  // we could add it back and come up with another way around this issue.
  // const stations = {};

  const [
    measurands,
    //credentials
  ] = await Promise.all([
    Measurand.getSupportedMeasurands(parameters),
    fetchSecret(source.provider)
  ]);

  // First we need to get the list of files to process
  //const files = await readdir(directoryPath);
  const files = [
    'locations.csv',
    'versions_v1.csv',
    'measurements_initial.csv',
    'measurements_v1.csv',
    'measurements_v1b.csv',
  ];


  // Next we are going to loop through them and
  // read them in as a json array
  await Promise.all(files.map( async (filename) => {
    let filepath = path.resolve(directoryPath, filename);
    let data = [];
    let file = {
      name: filename,
      path: filepath,
    };
    if(VERBOSE) console.log('processing file', filename);
    return new Promise((resolve, reject) => {
      fs.createReadStream(filepath)
        .pipe(csv())
        .on('data', row => data.push(row))
        .on('end', () => {
          // trying to remain consistent with other providers
          // so we are using the limit method
          // also, we are going to pass all files to the same process method
          // and let that method sort out which file it is.
          filesToProcess.push(
            limit(() => process({ file, data, measurands }))
          );
          resolve();
        });
    });
  }));

  // what is the point of this?
  //await Promise.all(Object.values(stations));
  //console.log(`ok - all ${Object.values(stations).length} fixed stations pushed`);

  await Promise.all(filesToProcess);
  console.log(`ok - all ${filesToProcess.length} files processed`);

}

/**
 *
 *
 * @param {object} file - file information to use for saving the final csv file
 * @param {array} data - the data, in client format, to be processed/normalized
 * @param {object} stations - an empty object to store station data (??)
 * @param {Measurands} measurands - list of the proper measurand names
 *
 * @returns {??}
 */
async function process({ file, data, measurands }) {

  const measures = new Measures(FixedMeasure);
  const sourceId = 'versioning';
  const versions = {};
  const stations = {};
  const undefines = [undefined, null, 'NaN', 'NA'];

  // even though we are supporting 3 file types we want to make sure that
  // we do not require a seperate version and location file if its not needed.
  for(const row of data) {
    let sensorNodeId = row.location;
    let { lifecycle, version } = row;
    // Compile the station information and check if it exists
    // If not this will add the sensor node to the stations directory
    // if it does exist then it will compare the json strings and possibly update.
    // Either a new file or an update will trigger an injest of the station data
    if(!stations[sensorNodeId]) {
      stations[sensorNodeId] = put_station(
        sourceId,
        new SensorNode({
          sensor_node_id: sensorNodeId,
          sensor_node_site_name: sensorNodeId,
          sensor_node_geometry: !undefines.includes(row.lat) ? [row.lng, row.lat] : null,
          sensor_node_country: row.country,
          sensor_node_city: row.city,
          sensor_node_source_name: sourceId,
          sensor_node_ismobile: row.ismobile,
          sensor_node_project: row.project,
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

    // Loop through the expected measurands
    for (const measurand of measurands) {
      const measure = row[measurand.input_param];
      const sensorId = getSensorId(sourceId, sensorNodeId, measurand.parameter, lifecycle, version);
      // we should check for a version now as we could have a version without a measure
      // Compile the version information and check if it exists
      // if not the version will be added to the versions directory
      // and trigger an import
      if(lifecycle && version) {
        if(!versions[sensorId]) {
          versions[sensorId] = put_version(
            new Version({
              parent_sensor_id: getSensorId(sourceId, sensorNodeId, measurand.parameter),
              sensor_id: sensorId,
              version_id: version,
              life_cycle_id: lifecycle,
              filename: file.name,
              readme: row.readme,
            })
          );
        }
      }
      // Now we can check for a measure and potentially skip
      if (undefines.includes(measure)) continue;
      // add the measurement to the measures
      measures.push({
        sensor_id: sensorId,
        measure: measurand.normalize_value(measure),
        timestamp: row.datetime,
      });
    }
  }

  // Now we can add any measurements created
  if(measures.length) {
    const filename = file.name.endsWith('.csv') ? file.name.slice(0, -4) : file.name;
    put_measures(sourceId, measures, filename);
  }

  // what should we return to the processor??
  return true;
}




module.exports = {
    processor,
};
