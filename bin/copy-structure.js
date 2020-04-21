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
    demand: ['srctable', 'desttable'],
    optional: ['srcenv', 'destenv'],
    usage: 'Copy Dynamo DB table structure from one AWS account to another AWS account\n' +
           'Usage: copy-structure --srctable table --desttable table [--srcenv srcenv] [--destenv destenv]\n\n'
});

var srcdynamo = utils.dynamo({
			table: argv.srctable,
			key: config.env[argv.srcenv].aws_access_key_id,
			secret: config.env[argv.srcenv].aws_secret_access_key,
			region: config.env[argv.srcenv].region
		});

var destdynamo = utils.dynamo({
			table: argv.desttable,
			key: config.env[argv.destenv].aws_access_key_id,
			secret: config.env[argv.destenv].aws_secret_access_key,
			region: config.env[argv.destenv].region
		});

srcdynamo.describeTable(
	{
		TableName: argv.srctable
	},
	function (err, data) {
		if (err != null) {
			throw err;
		}
		if (data == null) {
			throw 'Table ' + argv.table + ' not found in DynamoDB';
		}
		console.log("Source Schema");
		console.log(data);
		var dtable = {};
		dtable.AttributeDefinitions = data.Table.AttributeDefinitions;
		dtable.TableName = argv.desttable;
		dtable.KeySchema = data.Table.KeySchema;
		dtable.ProvisionedThroughput = {};
		dtable.ProvisionedThroughput.ReadCapacityUnits = data.Table.ProvisionedThroughput.ReadCapacityUnits > 0 ? data.Table.ProvisionedThroughput.ReadCapacityUnits : 1;
		dtable.ProvisionedThroughput.WriteCapacityUnits = data.Table.ProvisionedThroughput.WriteCapacityUnits > 0 ? data.Table.ProvisionedThroughput.WriteCapacityUnits : 1;
		console.log("\nDestination Schema");
		console.log(dtable);
		destdynamo.createTable(dtable, function(desterr, destdata) {
			if (desterr) {
				console.log("Error", desterr);
			} else {
				console.log("Table Created", destdata);
			}
		});
	}
);

