'use strict';

const fs = require('fs');
const path = require('path');
const cf = require('@mapbox/cloudfriend');
const schedule = require('./schedule');

const minute = [];
const hour = [];
const day = [];

for (const source_name of fs.readdirSync(path.resolve(__dirname, '../sources/'))) {
    const source = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../sources/', source_name)));

    if (source.frequency === 'minute') minute.push(path.parse(source_name).name);
    if (source.frequency === 'hour') hour.push(path.parse(source_name).name);
    if (source.frequency === 'day') day.push(path.parse(source_name).name);
}

const Parameters = {
    GitSha: {
        Type: 'String',
        Description: 'Gitsha to Deploy'
    },
    Bucket: {
        Type: 'String',
        Description: 'Bucket to write ETL data'
    },
    SecretPurpleAir: {
        Type: 'String',
        Description: 'PurpleAir API Token'
    }
};

const Resources = {
    LambdaFetcher: {
        Type: 'AWS::Lambda::Function',
        Properties: {
            Description: 'Fetch a single source for a given time period',
            Environment: {
                Variables: {
                    BUCKET: cf.ref('Bucket'),
                    STACK: cf.stackName
                    SecretPurpleAir: cf.ref('SecretPurpleAir'),
                }
            },
            Code: {
                S3Bucket: 'devseed-artifacts',
                S3Key: cf.join(['openaq-etl/lambda-', cf.ref('GitSha'), '.zip'])
            },
            Role: cf.getAtt('LambdaFetcherRole', 'Arn'),
            Handler: 'index.handler',
            MemorySize: 512,
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
            Path: '/',
            Policies: [{
                PolicyName: 'openaq-submit',
                PolicyDocument: {
                    Statement: [{
                        Effect: 'Allow',
                        Action: [
                            's3:*'
                        ],
                        Resource: cf.join(['arn:aws:s3:::', cf.ref('Bucket'), '/', cf.stackName, '/*'])
                    },{
                        Effect: 'Allow',
                        Action: [
                            'sqs:ReceiveMessage',
                            'sqs:DeleteMessage',
                            'sqs:GetQueueAttributes',
                            'sqs:ChangeMessageVisibility'
                        ],
                        Resource: cf.getAtt('FetcherQueue', 'Arn')
                    }]
                }
            }],
            ManagedPolicyArns: [
                'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
            ]
        }
    },
    FetcherQueue: {
        Type: 'AWS::SQS::Queue',
        Properties: {
            QueueName: cf.join('-', [cf.stackName, 'fetch-queue']),
            VisibilityTimeout: 2880
        }
    },
    FetchEventMap: {
        Type: 'AWS::Lambda::EventSourceMapping',
        Properties: {
            BatchSize: 1,
            Enabled: true,
            EventSourceArn: cf.getAtt('FetcherQueue', 'Arn'),
            FunctionName: cf.getAtt('LambdaFetcher', 'Arn')
        }
    }
};


module.exports = cf.merge({
    Parameters,
    Resources
},
schedule('Minute', minute, 'cron(* * * * ? *)'),
schedule('Hour', hour, 'cron(00 * * * ? *)'),
schedule('Day', day, 'cron(00 00 * * ? *)')
);
