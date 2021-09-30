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

Configuration for the ingestion is provided via environment variables.

- `BUCKET`: The bucket to which the ingested data should be written. **Required**
- `SOURCE`: The [data source](#data-sources) to ingest. **Required**
- `LCS_API`: The API used when fetching supported measurands. _Default: `'https://api.openaq.org'`_
- `STACK`: The stack to which the ingested data should be associated. This is mainly used to apply a prefix to data uploaded to S3 in order to separate it from production data. _Default: `'local'`_
- `SECRET_STACK`: The stack to which the used [Secrets](#provider-secrets) are associated. At times, a developer may want to use credentials relating to a different stack (e.g. a devloper is testing the script, they want output data uploaded to the `local` stack but want to use the production stack's secrets). _Default: the value from the `STACK` env variable_
- `VERBOSE`: Enable verbose logging. _Default: disabled_

### Running locally

To run the ingestion script locally (useful for testing without deploying), see the following example:

```sh
LCS_API=https://api.openaq.org \
STACK=my-dev-stack \
SECRET_STACK=my-prod-stack \
BUCKET=openaq-fetches \
VERBOSE=1 \
SOURCE=habitatmap \
node fetcher/index.js
```

## Data Sources

Data Sources can be configured by adding a config file & corresponding provider script. The two sections below
outline what is necessary to create and a new source.

### Source Config

The first step for a new source is to add JSON config file to the the `fetcher/lib/sources` directory.

```json
{
  "schema": "v1",
  "provider": "example",
  "frequency": "hour",
  "meta": {}
}
```

| Attribute   | Note                       |
| ----------- | -------------------------- |
| `provider`  | Unique provider name       |
| `frequency` | `day`, `hour`, or `minute` |

The config file can contain any properties that should be configurable via the
provider script. The above table however outlines the attributes that are required.

### Provider Script

The second step is to add a new provider script to the `fetcher/lib/providers` directory.

The script here should expose a function named `processor` and should pass
`SensorSystem` & `Measures` objects to the `Providers` class.

The script below is a basic example of a new source:

```js
const Providers = require("../providers");
const { Sensor, SensorNode, SensorSystem } = require("../station");
const { Measures, FixedMeasure, MobileMeasure } = require("../measure");

async function processor(source_name, source) {
  // Get Locations/Sensor Systems via http/s3 etc.
  const locs = await get_locations();

  // Map locations into SensorNodes
  const station = new SensorNode();

  await Providers.put_stations(source_name, [station]);

  const fixed_measures = new Measures(FixedMeasure);
  // or
  const mobile_measures = new Measures(MobileMeasure);

  fixed_measures.push(
    new FixedMeasure({
      sensor_id: "PurpleAir-123",
      measure: 123,
      timestamp: Math.floor(new Date() / 1000), //UNIX Timestamp
    })
  );

  await Providers.put_measures(source_name, fixed_measures);
}

module.exports = { processor };
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
