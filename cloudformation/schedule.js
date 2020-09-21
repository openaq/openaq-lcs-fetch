'use strict';

const cf = require('@mapbox/cloudfriend');

function schedule(name, sources, cron) {
    const stack =  {
        Resources: { }
    };

    stack.Resources[`${name}LambdaScheduleRole`] = {
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
    };

    stack.Resources[`${name}LambdaSchedule`] = {
        Type : 'AWS::Events::Rule',
        Properties: {
            ScheduleExpression: cron,
            State: 'ENABLED',
            Targets: [{
                Id: `${name}LambdaSchedule`,
                Arn: cf.getAtt(`${name}LambdaScheduleFunction`, 'Arn')
            }]
        }
    };

    stack.Resources[`${name}LambdaSchedulePermission`] = {
        Type: 'AWS::Lambda::Permission',
        Properties: {
            Action: 'lambda:InvokeFunction',
            FunctionName: cf.getAtt(`${name}LambdaScheduleFunction`, 'Arn'),
            Principal: 'events.amazonaws.com',
            SourceArn: cf.getAtt(`${name}LambdaSchedule`, 'Arn')
        }
    };

    stack.Resources[`${name}LambdaScheduleFunction`] = {
        Type: 'AWS::Lambda::Function',
        Properties: {
            Description: `OpenAQ ${name} Fetcher`,
            Code: {
                ZipFile: `
                    function handler() {
                        const AWS = require('aws-sdk');
                        const sources = JSON.parse('${JSON.stringify(sources)}');

                        //TODO Write to SQS
                    }

                    module.exports.handler = handler;
                `
            },
            Handler: 'index.handler',
            MemorySize: 128,
            Role: cf.getAtt(`${name}LambdaScheduleRole`, 'Arn'),
            Runtime: 'nodejs12.x',
            Timeout: '25'
        }
    };

    return stack;
}

module.exports = schedule;
