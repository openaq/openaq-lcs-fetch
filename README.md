<h1 align=center>OpenAQ-ETL</h1>

<p align=center>Perform ETL into the OpenAQ Low Cost Sensor Database</p>

## Deploy

Before you are able to deploy infrastructure you must first setup the [OpenAddresses Deploy tools](https://github.com/openaddresses/deploy)

Once these are installed, you can create a stack via:

```sh
deploy create demo
```

Or update to the latest GitSha or CloudFormation template via:

```sh
deploy update production
```

Alternatively if you do not want to install the deploy tools, a raw CloudFormation JSON can be compiled by running the following:

```sh
yarn -s deploy
```

This JSON document can then be uploaded with standard AWS tools

### Parameters

#### GitSha

The GitSha to deploy. Each time a new commit is pushed, github actions will automatically
build all required lambda/docker resources and push them to s3.

#### Bucket

The bucket to push station and measure data to. Differing stacks can share a single bucket as data will be prefixed.

```
s3://{bucket}/{stack-name}/
```

#### SecretPurpleAir

The Purple API Token (Can be obtained from OpenAQ stafff)

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

The first step for a new source is to add the config file located in the `sources/` directory.


```json
{
    "provider": "example",
    "frequency": "hour"
}
```

| Attribute   | Note                       |
| ----------- | -------------------------- |
| `provider`  | Unique provider name       |
| `frequency` | `day`, `hour`, or `minute` |


The config file can contain any properties that should be configurable via the
provider script. The above table however outlines the attributes that are required.

Note that the source should be named `{provider}.json`

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
