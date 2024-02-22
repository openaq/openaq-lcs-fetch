const zlib = require('zlib');
const { promisify } = require('util');
const request = promisify(require('request'));
const AWS = require('aws-sdk');
const gzip = promisify(zlib.gzip);
const unzip = promisify(zlib.unzip);
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');
//const stripBomStream = import('strip-bom-stream');
const readdir = promisify(fs.readdir);
const mv = promisify(fs.rename);


const s3 = new AWS.S3({
  maxRetries: 10
});

const { Storage } = require('@google-cloud/storage');

const VERBOSE = !!process.env.VERBOSE;
const DRYRUN = !!process.env.DRYRUN;

// global storage object
var storage;

const revokeCredentials = () => {
  storage = null;
};

const applyCredentials = credentials => {
  if(!storage && credentials) {
    if (!credentials.client_email) throw new Error('client_email required');
    if (!credentials.client_id) throw new Error('client_id required');
    if (VERBOSE) console.debug(`Initializing storage object '${credentials.project_id}' as ${credentials.client_email}`);
    let projectId = credentials.project_id;
    // https://github.com/googleapis/google-cloud-node/blob/main/docs/authentication.md
    storage = new Storage({
      projectId,
      credentials
    });
  }
};

/**
 * Retrieve secret from AWS Secrets Manager
 * @param {string} source_name The source for which we are fetching a secret.
 *
 * @returns {object}
 */
async function fetchSecret(source_name) {

  const region = process.env.AWS_DEFAULT_REGION || 'us-west-2';
  const stack = process.env.SECRET_STACK || process.env.STACK;
  const SecretId = `${stack}/${source_name}`;
  // make sure we have a stack name
  if (!stack) throw new Error('STACK Env Var Required');

  const secretsManager = new AWS.SecretsManager({
    region,
  });

  if (VERBOSE) console.debug(`Fetching secret - ${region}/${SecretId}...`);

  const { SecretString } = await secretsManager.getSecretValue({
    SecretId
  }).promise()
        .catch( err => {
          // this is a stop gap until there is a way to say there is
          // nothing to lookup
          if (VERBOSE) console.debug(err);
          return { SecretString: '{}' };
        });

  return JSON.parse(SecretString);
}

const getObject = async (Key) => {
  const Bucket = process.env.BUCKET;
  //console.debug('GETTING OBJECT', `${Bucket}/${Key}`);
  var data;
  try {
    const resp = await s3.getObject({ Bucket, Key }).promise();
    data = (await unzip(resp.Body)).toString('utf-8');
  } catch (err) {
    if (err.statusCode !== 404) throw err;
  }
  return data;
};

const putObject = async (data, Key) => {
  const Bucket = process.env.BUCKET;
  var ContentType = 'application/json';

  if(data.constructor.name === 'Object') {
    data = JSON.stringify(data);
  } else if(data.constructor.name === 'Measures') {
    data = await gzip(data.csv());
    ContentType = 'text/csv';
  } else {
    console.warn('Writing object of unknown type',  data.constructor.name, typeof data, typeof data === 'string', data);
  }

  if(data.constructor.name !== 'Buffer') {
    data = await gzip(data);
  }

  //if(!DRYRUN) {
    if (VERBOSE) console.debug(`Saving data to ${Key}`);
    await s3.putObject({
      Bucket,
      Key,
      Body: data,
      ContentType,
      ContentEncoding: 'gzip',
    }).promise().catch( err => {
      console.log('error putting object', err);
    });
  //} else {
  //  if (VERBOSE) console.debug(`Would have saved data to ${Key}`);
  //  await writeJson(data, Key);
  //}
};

const writeJson = async (data, filepath) => {
  const dir = process.env.LOCAL_DESTINATION_BUCKET || __dirname;
  if (VERBOSE) console.debug('writing data to local directory', dir, filepath);
  let jsonString = data;
  if(typeof(data) == 'object' && !Buffer.isBuffer(data)) {
    jsonString = path.extname(filepath) === ".gz"
      ? await gzip(JSON.stringify(data))
      : JSON.stringify(data);
  }
  //await fs.promises.writeFile(filepath, jsonString);
  const fullpath = path.join(dir, filepath);
  fs.promises.mkdir(path.dirname(fullpath), {recursive: true})
    .then( res => {
      fs.writeFileSync(fullpath, jsonString);
    }).catch( err => {
      console.warn(err);
    });
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


/////////////////////////////////////
// Methods for lising source files //
/////////////////////////////////////


const listFiles = async (source) => {
  const type = source.type;
  if(type === 'google-bucket'){
	  return await listFilesGoogleBucket(source.config);
  } else {
	  return await listFilesLocal(source.config);
  }
};

const listFilesGoogleBucket = async config => {
  applyCredentials(config.credentials);
  const bucket = config.bucket;
  const f = [];

  const options = {
	  prefix: config.prefix || 'pending/',
	  delimeter: '/',
  };

  if (VERBOSE) console.debug(`Fetching file list from '${bucket}/${options.prefix}'`);
  const [files] = await storage.bucket(bucket).getFiles(options);

  files.forEach(file => {
    if(!file.name.endsWith('/')) {
      if (VERBOSE) console.debug('listing file', file.name);
	    f.push({
        source: 'google-bucket',
	      path: file.name,
	      name: path.basename(file.name),
	      id: file.id,
	      bucket,
	    });
    } else {
      if (VERBOSE) console.debug('Skipping directory path', file.name);
    }
  });
  return f;
};

const listFilesLocal = async (config) => {
  const dir = process.env.LOCAL_SOURCE_BUCKET || __dirname;
  const { folder } = config;
  const directoryPath = path.join(dir, folder);
  if (VERBOSE) console.debug(`Fetching local file list from '${directoryPath}'`);
  const list = await readdir(directoryPath);
  const files = list.map( filename => ({
    source: 'local',
	  name: filename,
	  path: path.join(directoryPath, filename),
  }));
  return files;
};




///////////////////////////////////////
// Methods for fetching source files //
///////////////////////////////////////



const fetchFile = async file => {
  const source = file.source;
	var data;
  if(source == 'google-bucket') {
	  data = await fetchFileGoogleBucket(file);
  } else {
	  data = await fetchFileLocal(file);
  }
	if(DRYRUN) {
			writeJson(data, `raw/${file.path}`)
	}
	return data;
};

const fetchFileLocal = async file => {
  const filepath = file.path;
  const data = [];
  //console.log(file)
  const stripBomStream = await import('strip-bom-stream');
  return new Promise((resolve, reject) => {
	  fs.createReadStream(filepath)
      .pipe(stripBomStream.default())
      .pipe(csv())
      .on('data', row => data.push(row))
      .on('end', () => {
		    resolve(data);
      });
  });
};

const fetchFileGoogleBucket = async file => {
  const data = [];
  const stripBomStream = await import('strip-bom-stream');
  return new Promise((resolve, reject) => {
    storage
	    .bucket(file.bucket)
	    .file(file.path)
	    .createReadStream() //stream is created
      .pipe(stripBomStream.default())
      .pipe(csv())
      .on('data', row => data.push(row))
      .on('end', () => {
	      resolve(data);
      });
  });
};


/////////////////////////////////////
// Methods for moving source files //
/////////////////////////////////////



const moveFile = async (file, destDirectory) => {
  const source = file.source;
  if (DRYRUN) {
    return file;
  } else if(source == 'google-bucket') {
	  return await moveFileGoogleBucket(file, destDirectory);
  } else {
	  return await moveFileLocal(file, destDirectory);
  }
};

const moveFileGoogleBucket = async (file, destDirectory) => {
  if (VERBOSE) console.debug(`Moving google file to ${destDirectory}`, file);
  const dest = path.join(destDirectory, file.name);
  await storage.bucket(file.bucket).file(file.path).move(dest);
  file.path = dest;
  return file;
};

const moveFileLocal = async (file, destDirectory) => {
  const orig = file.path;
  destDirectory = path.join(path.dirname(path.dirname(file.path)), destDirectory);
  const dest = path.join(destDirectory, file.name);
  if (VERBOSE) console.debug(`Moving local file to ${dest}`);
  await mv(orig, dest);
  file.path = dest;
  return file;
};


const writeError = async (file) => {
  const source = file.source;
  // rename the file for the error
  const parsed = path.parse(file.path);
  const dir = path.dirname(parsed.dir);
  file.path = `${dir}/errors/${parsed.name}_error.txt`;

  if (DRYRUN) {
	  return await writeErrorLocal(file);
    //return file;
  } else if(source == 'google-bucket') {
	  return await writeErrorGoogleBucket(file);
  } else {
	  return await writeErrorLocal(file);
  }
};

const writeErrorGoogleBucket = async (file) => {
  if (VERBOSE) console.debug(`Writing google error to ${file.path}`, file);
  //const dest = path.join(destDirectory, file.name);
  const path = file.path.replace('./','');
  await storage.bucket(file.bucket).file(path).save(file.error);
  return file;
};

const writeErrorLocal = async (file) => {
  const dir = process.env.LOCAL_DESTINATION_BUCKET || __dirname;
  const fullpath = path.join(dir, file.path);
  if (VERBOSE) console.debug(`Writing local error to ${fullpath}`, file);
  fs.promises.mkdir(path.dirname(fullpath), {recursive: true})
    .then( res => {
				fs.writeFileSync(fullpath, file.error);
    }).catch( err => {
      console.warn(err);
    });
  return file;
};


/**
 * Transform phrase to camel case.
 * e.g. toCamelCase("API Key") === "apiKey"
 *
 * @param {string} phrase
 * @returns {string}
 */
function toCamelCase(phrase) {
    return phrase
        .split(' ')
        .map((word) => word.toLowerCase())
        .map((word, i) => {
            if (i === 0) return word;
            return word.replace(/^./, word[0].toUpperCase());
        })
        .join('');
}


/**
 * Print out JSON station object
 * @param {obj} station
 */
function prettyPrintStation(station) {
    if (typeof(station) === 'string') {
        station = JSON.parse(station);
    }
    for (const [key, value] of Object.entries(station)) {
        if (key !== 'sensor_systems') {
            console.log(`${key}: ${value}`);
        } else {
            console.log('Sensor systems');
            value.map( (ss) => {
                for (const [ky, vl] of Object.entries(ss)) {
                    if (ky !== 'sensors') {
                        console.log(`-- ${ky}: ${vl}`);
                    } else {
                        vl.map((s) => console.debug(`---- ${s.sensor_id} - ${s.measurand_parameter} ${s.measurand_unit}`));
                    }
                }
            });
        }
    }
}


module.exports = {
  fetchSecret,
  applyCredentials,
  revokeCredentials,
  request,
  VERBOSE,
  DRYRUN,
  putObject,
  getObject,
  moveFile,
  fetchFile,
  listFiles,
  readJson,
  writeJson,
  writeError,
  toCamelCase,
  gzip,
  unzip,
  prettyPrintStation,
};
