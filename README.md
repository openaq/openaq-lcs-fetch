<h1 align=center>OpenAQ-ETL</h1>

<p align=center>Perform ETL into the OpenAQ Low Cost Sensor Database</p>

## Deploy

- `yarn cdk deploy` deploy this stack to your default AWS account/region
- `yarn cdk diff` compare deployed stack with current state
- `yarn cdk synth` emits the synthesized CloudFormation template

## Development

Javascript Documentation can be obtained by running the following

```
yarn doc
```

Tests can be run using the following

```
yarn test
```

### Env Variables

Production configuration for the ingestion is provided via environment variables.

- `BUCKET`: The bucket to which the data to be ingested should be written. **Required**
- `API_URL`: The API used when fetching supported measurands. _Default: `'https://api.openaq.org'`_
- `STACK`: The stack to which the ingested data should be associated. This is mainly used to apply a prefix to data uploaded to S3 in order to separate it from production data and to pull secrets down. _Default: `'local'`_

Development configuration adds the following variables
- `SOURCE`: The [data source](#data-sources) to ingest. This is provided from the file object in production but required to be set during development. Do not set this variable in production. **Required**
- `SOURCE_TYPE`: This will override the type value set in the source config
- `LOCAL_SOURCE_BUCKET`: This is (most likely) the path to use when pulling files from a local directory. Very helpful as you are setting up a new source.
- `LOCAL_DESTINATION_BUCKET`: The local path to save the transformed files. Also helpful when debugging pipelines.
- `DRYRUN`: If set to truthy it will prevent any upload or moving of files but stil allow you to get them.
- `VERBOSE`: Enable verbose logging. _Default: disabled_

### Running locally

To run the ingestion script locally (useful for testing without deploying), see the following example:

```sh
API_URL=https://api.openaq.org \
SOURCE=cac \
SOURCE_TYPE=local \
STACK=my-dev-stack \
BUCKET=openaq-fetches \
VERBOSE=1 \
DRYRUN=1 \
node fetcher/index.js
```

## Data Sources

Data Sources can be configured by adding a config file & corresponding provider script. The two sections below
outline what is necessary to create and a new source.

### Source Config

The first step for a new source is to add JSON config file to the the `fetcher/sources` directory.

```json
{
  "name": "cac",
  "schema": "v1",
  "provider": "versioning",
  "frequency": "hour",
  "type": "google-bucket",
  "config": {
    "bucket": "name-of-bucket",
    "folder": "path/to/files",
    ...
  },
  "parameters": {
    "pm25": ["pm25", "ppm"],
    ...
   }
  "
}
```

Attributes
`name`: A unique name for reference
`provider`: The provider script to use
`frequency`: How often to run the source
`type`: the source location type. Currently supports `google-bucket` and local
`config`: Any config parameters needed for the source location
`parameters`: A list of accepted measurands and their mapped value and units

The config file can contain any properties that should be configurable via the
provider script. The above table however outlines the attributes that are required.

### Provider Script

The second step is to add a new provider script to the `fetcher/providers` directory.

The script here should expose a function named `processor`. This function should pass
`SensorSystem` & `Measures` objects to the `Providers` class.

The script below is a basic example of a new source:

```json
{
  "name": "source_name",
  "schema": "v1",
  "provider": "versioning",
  "frequency": "minute",
  "type": "google-bucket",
  "config" : {
    "bucket":"location-bucket",
    "folder":"pending"
  },
  "parameters": {
    "pm25": ["pm25", "ppb"],
    "pm10": ["pm10", "ppb"],
    "temp": ["temperature", "c"],
    "ws": ["wind_speed", "m/s"],
    "wd": ["wind_direction", "deg"]
  }
}
```

### Provider Secrets

For data providers that require credentials, credentials should be store on AWS Secrets Manager with an ID composed of the stack name and provider name, such as `:stackName/:providerName`.

#### Google Keys

Some providers (e.g. CMU, Clarity) require us to read data from Google services (e.g. Drive, Sheets). To do this, the organization hosting the data should do the following:

1. [create a project & enable access to the required APIs](https://developers.google.com/workspace/guides/create-project)
1. [create a service account](https://cloud.google.com/iam/docs/creating-managing-service-accounts)
1. [generate service account keys](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)

The should look something like the following and be stored in its entirety within the AWS Secrets Manager.

```json
{
  "type": "service_account",
  "project_id": "project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n",
  "client_email": "service-account-email",
  "client_id": "client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-email"
}
```
