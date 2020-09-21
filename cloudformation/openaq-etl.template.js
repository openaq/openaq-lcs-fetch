'use strict';

const fs = require('fs');
const path = require('path');
const cf = require('@mapbox/cloudfriend');
const schedule = require('./schedule');

const minute = [];
const hour = [];
const day = [];

for (let source of fs.readdirSync(path.resolve(__dirname, '../sources/'))) {
    source = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../sources/', source)))

}

const Parameters = {
    GitSha: {
        Type: 'String',
        Description: 'Gitsha to Deploy'
    }
};

const Resources = {
    LambdaFetcher: {
        Type: 'AWS::Lambda::Function',
        Description: 'Fetch a single source for a given time period',
        Code: {
            S3Bucket: 'devseed-artifacts',
            S3Key: cf.join(['openaq-etl/', cf.ref('GitSha')])
        },
        Handler: 'index.handler',
        MemorySize: 128,
        Runetime: 'nodejs12.x',
        Timeout: '900'
    }
};


module.exports = cf.merge({
    Parameters,
    Resources,
},
    schedule('Minute', minute, '* * ? * * *'),
    schedule('Hour', hour, '0 * ? * * *'),
    schedule('Day', day, '0 0 * * ?  *')
);
