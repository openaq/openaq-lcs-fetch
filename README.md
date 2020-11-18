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
