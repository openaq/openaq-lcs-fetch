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
  fetchSecret,
  revokeCredentials,
  getObject,
  putObject,
  listFiles,
  fetchFile,
  moveFile,
  writeError,
} = require('../lib/utils');


// we are going to process files in batches and then
// save everything all at once
var versions = {}; // for tracking versions
var stations = {}; // for keeping track of new stations
var sensors = {};  // for tracking new sensors
var measures_list = [];


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
function getSensorId(sourceId, sensorNodeId, measurandParameter, lifeCycle, versionDate) {
  const lifeCycleId = !!lifeCycle
        ? `-${lifeCycle}`
        : '';
  // if no lifecyle value is provided we should assume this is raw data
  const versionId = versionDate && !!lifeCycle
        ? `-${versionDate}`
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
 * @param {object} source - An object that also includes the source name?
 *
 * @returns {string}
 */
async function processor(source) {

  process.env.PROVIDER = source.name;

  // reset global variables
  versions = {};
  stations = {};
  sensors = {};
  measures_list = [];
  revokeCredentials();

  const [
    measurands,
    credentials
  ] = await Promise.all([
    Measurand.getSupportedMeasurands(source.parameters),
    fetchSecret(source.name)
  ]);

  if(credentials) {
    // add the credentials to the source config
    if (VERBOSE) console.debug("Secret credentials", credentials);
    source.config.credentials = credentials;
  }

  // First we need to get the list of files to process
  const files = await listFiles(source);
  if (VERBOSE) console.debug(`Processing ${files.length} files for ${source.name}`);

  // Next we are going to loop through them and
  // read them in as a json array
  await Promise.all(files.map( async (file) => {
	  const data = await fetchFile(file);
	  return processFile({ file, data, measurands })
      .then( res => {
	      moveFile(file, 'processed');
        return res;
      })
      .catch( err => {
        console.error(`File processing error: ${err.message}`);
        writeError({
          ...file,
          error: `**FILE ERROR**\nFILE: ${file.path}\nDETAILS: ${err.message}`,
        });
	      moveFile(file, 'errors');
      });
  }));

  // now we should put the stations, sensors and versions
  if (VERBOSE) console.debug(`Saving ${Object.keys(stations).length} station(s)`);
  Object.values(stations).map(s => {
    s.put();
  });

  if (VERBOSE) console.debug(`Saving ${Object.keys(versions).length} version(s)`);
  Object.values(versions).map(v => {
    v.put();
  });

  if (VERBOSE) console.debug(`Saving ${measures_list.length} set(s) of measurements`);
  Object.values(measures_list).map(m => {
    m.put();
  });

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
async function processFile({ file, data, measurands }) {
  if (VERBOSE) console.debug('Processing file', file.path);
  const measures = new Measures(FixedMeasure, file);
  const sourceId = 'versioning';

  const undefines = [undefined, null, 'NaN', 'NA'];

  // we are supporting a few different file structures at this point
  // all of them are csv and so they should be arrays when we reach this point
  // and so the best method for distinguishing what to do here is going to be
  // based on the fields that are available in the array/row
  for(const row of data) {
    // every row we encounter has to include the station/location info
    if(!row.location) throw new Error('No location field found');
    if (VERBOSE) console.log(`Processing location: ${row.location}`, row);
    let sensorNodeId = row.location;
    let station = stations[sensorNodeId];

    // Compile the station information and check if it exists
    // If not this will add the sensor node to the stations directory
    // if it does exist then it will compare the json strings and possibly update.
    // Either a new file or an update will trigger an injest of the station data
    if(!station) {
      station = new SensorNode({
        sensor_node_id: sensorNodeId,
        sensor_node_site_name: sensorNodeId,
        sensor_node_source_name: sourceId,
        sensor_node_geometry: !undefines.includes(row.lat) ? [row.lng, row.lat] : null,
        ...row,
      });
      stations[sensorNodeId] = station;
    }

    // Loop through the expected measurands and check
    // to see if we have a column that matches, this would be for a measurement
    // use this method for the versions and sensor files as well because
    // its as good a method as any to match the parameter to the measurand
    for (const measurand of measurands) {
      // if we have a parameter column we assume this is either
      // a versioning file or a sensor meta data file
      // in either case each row will be a new sensor id
      if(row.parameter && measurand.input_param!==row.parameter) {
        //console.log('parameter file, skiping', parameter, measurand.input_param);
        continue;
      }

      const measure = row[measurand.input_param];

      // build the sensor id
      const sensorId = getSensorId(
        sourceId,
        sensorNodeId,
        measurand.parameter,
        row.lifecycle,
        row.version_date,
      );

      let sensor = sensors[sensorId];

      if(!sensor) {
        sensor = new Sensor({
          sensor_id: sensorId,
          measurand_parameter: measurand.parameter,
          measurand_unit: measurand.normalized_unit,
          ...row,
        });
        station.addSensor(sensor);
        sensors[sensorId] = sensor;
      }

      // we should check for a version now as we could have a version without a measure
      // Compile the version information and check if it exists
      // if not the version will be added to the versions directory
      // and trigger an import

      if(row.lifecycle && (row.version_date || row.version)) {
        if(!versions[sensorId]) {
          versions[sensorId] = new Version({
            parent_sensor_id: getSensorId(
              sourceId,
              sensorNodeId,
              measurand.parameter
            ),
            sensor_id: sensorId,
            version_id: row.version_date || row.version,
		        life_cycle_id: row.lifecycle,
		        parameter: measurand.parameter,
            filename: file.name,
            readme: row.readme,
            provider: sourceId,
          });
        }
      }
      // Now we can check for a measure and potentially skip
      if (undefines.includes(measure)) continue;
      // make sure that we have this sensor

      // add the measurement to the measures
      measures.push({
        sensor_id: sensorId,
        measure: measurand.normalize_value(measure),
        timestamp: row.datetime,
      });
    }
  }

  // And then we can add any measurements created
  if(measures.length) {
    measures_list.push(measures);
  }

  // what should we return to the processor??
  return true;
}




module.exports = {
  processor,
};
