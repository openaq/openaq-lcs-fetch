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
        Properties: {
            Description: 'Fetch a single source for a given time period',
            Code: {
                S3Bucket: 'devseed-artifacts',
                S3Key: cf.join(['openaq-etl/lambda-', cf.ref('GitSha'), '.zip'])
            },
            Role: cf.getAtt(`LambdaFetcherRole`, 'Arn'),
            Handler: 'index.handler',
            MemorySize: 128,
            Runtime: 'nodejs12.x',
            Timeout: '900'
        }
    },
    LambdaFetcherRole: {
        Type: 'AWS::IAM::Role',
        Properties: {
            AssumeRolePolicyDocument: {
                Version: '2012-10-17',
                Statement: [{
                    Effect: 'Allow',
                    Principal: {
                        Service: ['lambda.amazonaws.com']
                    },
                    Action: ['sts:AssumeRole']
                }]
            },
            ManagedPolicyArns: [
                'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
            ]
        }
    }
};


module.exports = cf.merge({
    Parameters,
    Resources,
},
    schedule('Minute', minute, 'cron(* * * * ? *)'),
    schedule('Hour', hour, 'cron(00 * * * ? *)'),
    schedule('Day', day, 'cron(00 00 * * ? *)')
);
