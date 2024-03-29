#!/usr/bin/env node
/**
 * Copyright 2020 T.N.Satish
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var utils = require('../lib/utils');
const fs = require('fs');

var configData = fs.readFileSync('config.json');
var config = JSON.parse(configData);

var argv = utils.config({
    demand: ['table'],
    optional: ['key', 'secret', 'region', 'env'],
    usage: 'Copy Dynamo DB tables from one AWS account to another AWS account\n' +
           'Usage: describe --table table [--region us-east-1] [--key AK...AA] [--secret 7a...IG] [--env env]\n\n'
});

var dynamo = utils.dynamo({
			table: argv.table,
			key: argv.key || config.env[argv.env].aws_access_key_id,
			secret: argv.secret || config.env[argv.env].aws_secret_access_key,
			region: argv.region || config.env[argv.env].region,
			endpointUrl: argv.endpointUrl || config.env[argv.env].endpointUrl
		});

dynamo.describeTable(
    {
        TableName: argv.table
    },
    function (err, data) {
        if (err != null) {
            throw err;
        }
        if (data == null) {
            throw 'Table ' + argv.table + ' not found in DynamoDB';
        }
	console.log(data);
	console.log(JSON.stringify(data));
    }
);

