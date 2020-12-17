<h1 align=center>OpenAQ-ETL</h1>

<p align=center>Perform ETL into the OpenAQ Low Cost Sensor Database</p>

## Deploy

 * `yarn cdk deploy`      deploy this stack to your default AWS account/region
 * `yarn cdk diff`        compare deployed stack with current state
 * `yarn cdk synth`       emits the synthesized CloudFormation template

### Parameters

#### Bucket

The bucket to push station and measure data to. Differing stacks can share a single bucket as data will be prefixed.

```
s3://{bucket}/{stack-name}/
```

### Secrets

For data providers that require credentials, credentials should be store on AWS Secrets Manager with an ID composed of the stack name and provider name, such as `:stackName/:providerName`.

## Development

Javascript Documentation can be obtained by running the following

```
yarn doc
```

Tests can be run using the following

```
yarn test
```

## Data Sources

Data Sources can be configured by adding a config file & corresponding provider script. The two sections below
outline what is necessary to create and a new source.

### Source Config

The first step for a new source is to add an entry to the array located in the `lib/sources.js` directory.


```json
{
    "schema": "v1",
    "provider": "example",
    "frequency": "hour",
    "meta": {

    }
}
```

| Attribute   | Note                       |
| ----------- | -------------------------- |
| `provider`  | Unique provider name       |
| `frequency` | `day`, `hour`, or `minute` |


The config file can contain any properties that should be configurable via the
provider script. The above table however outlines the attributes that are required.

### Provider Script

The second step is to add a new provider script to the `lib/providers/` directory.

The script here should expose a function named `processor` and should pass
`SensorSystem` & `Measures` objects to the `Providers` class.

The script below is a basic example of a new source:


```js
const Providers = require('../providers');
const { Sensor, SensorNode, SensorSystem } = require('../station');
const { Measures, FixedMeasure, MobileMeasure } = require('../measure');

async function processor(source_name, source) {
    // Get Locations/Sensor Systems via http/s3 etc.
    const locs = await get_locations()

    // Map locations into SensorNodes
    const station = new SensorNode();

    await Providers.put_stations(source_name, [ station ]);

    const fmeasures = new Measures(FixedMeasure);
    // or
    const mmeasures = new Measures(MobileMeasure);

    fmeasures.push(new FixedMeasure({
        sensor_id: 'PurpleAir-123',
        measure: 123,
        timestamp: Math.floor(new Date() / 1000) //UNIX Timestamp
    }));

    await Providers.put_measures(source_name, fmeasures);
}

module.exports = { processor };
```
