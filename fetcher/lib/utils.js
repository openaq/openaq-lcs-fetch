const { promisify } = require('util');
const request = promisify(require('request'));
const AWS = require('aws-sdk');
const zlib = require('zlib');
const gzip = promisify(zlib.gzip);
const unzip = promisify(zlib.unzip);
const path = require('path');
const fs = require('fs');
const csv = require('csv-parser');

const s3 = new AWS.S3({
  maxRetries: 10
});

const { Storage } = require('@google-cloud/storage');

const VERBOSE = !!process.env.VERBOSE;
// global storage object
var storage;

const revokeCredentials = () => {
  storage = null;
};

const applyCredentials = credentials => {
  if(!storage && credentials) {
    console.debug(`Initializing storage object '${credentials.project_id}' as ${credentials.client_email}`);
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
  //console.debug('PUTTING OBJECT',  data.constructor.name, `${Bucket}/${Key}`);

  if(data.constructor.name === 'Object') {
    data = JSON.stringify(data);
  } else if(data.constructor.name === 'Measures') {
    data = await gzip(data.csv());
  } else {
    console.warn('Writing object of unknown type',  data.constructor.name, typeof data, typeof data === 'string', data);
  }

  if(data.constructor.name !== 'Buffer') {
    data = await gzip(data);
  }

  await s3.putObject({
    Bucket,
    Key,
    Body: data,
    ContentType: 'application/json',
    ContentEncoding: 'gzip',
  }).promise();
};

const writeJson = async (data, filepath) => {
  console.debug('writing data to text', filepath);
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
	  prefix: 'pending/',
	  delimeter: '/',
  };

  console.debug(`Fetching file list from '${bucket}'`);
  const [files] = await storage.bucket(bucket).getFiles(options);

  files.forEach(file => {
	  f.push({
      source: 'google-bucket',
	    path: file.name,
	    name: path.basename(file.name),
	    id: file.id,
	    bucket,
	  });
  });
  return f;
};

const listFilesLocal = async (config) => {
  const { directory } = config;
  const directoryPath = path.join(__dirname, directory, 'staging/pending');
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
  if(source == 'google-bucket') {
	  return await fetchFileGoogleBucket(file);
  } else {
	  return await fetchFileLocal(file);
  }
};

const fetchFileLocal = async file => {
  const filepath = file.path;
  const data = [];
  return new Promise((resolve, reject) => {
	  fs.createReadStream(filepath)
      .pipe(csv())
      .on('data', row => data.push(row))
      .on('end', () => {
		    resolve(data);
      });
  });
};

const fetchFileGoogleBucket = file => {
  const data = [];
  return new Promise((resolve, reject) => {
    storage
	    .bucket(file.bucket)
	    .file(file.path)
	    .createReadStream() //stream is created
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
  console.debug(`Moving file to ${destDirectory}`, file);
  if(source == 'google-bucket') {
	  return await moveFileGoogleBucket(file, destDirectory);
  } else {
	  return await moveFileLocal(file, destDirectory);
  }
};

const moveFileGoogleBucket = async (file, destDirectory) => {
  console.debug(`Moving google file to ${destDirectory}`, file);
  const dest = path.join(destDirectory, file.name);
  await storage.bucket(file.bucket).file(file.path).move(dest);
  file.path = dest;
  return file;
};

const moveFileLocal = async (file, destDirectory) => {
  const dest = path.join(destDirectory, file.name);
  file.path = dest;
  return file;
};



const getFile = async (Key) => {
  const Bucket = process.env.BUCKET;
};

const putFile = async (data, filepath) => {
  const Bucket = process.env.BUCKET;
};


module.exports = {
  fetchSecret,
  applyCredentials,
  revokeCredentials,
  request,
  VERBOSE,
  putObject,
  getObject,
  moveFile,
  fetchFile,
  listFiles,
  readJson,
  writeJson,
};
