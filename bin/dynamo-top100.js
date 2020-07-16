#!/usr/bin/env node
/**
 * Copyright 2013 Yegor Bugayenko
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
var sleep = require('system-sleep');
const fs = require('fs');

var argv = utils.config({
	demand: ['table'],
	optional: ['rate', 'query', 'key', 'secret', 'region', 'index', 'env'],
	usage: 'Gets Top N records from Dynamo DB table to standard output in JSON\n' +
	'Usage: dynamo-archive --table my-table [--rate 100] [--query "{}"] [--region us-east-1] [--key AK...AA] [--secret 7a...IG] [--index index-name]\n' + 
	'Usage: dynamo-archive --table my-table [--env env] \n\n' + 
	'Use --rate N to get top N records\n'
});

var configData = fs.readFileSync('config.json');
var config = JSON.parse(configData);
var rate = argv.rate || config.rate || 100;

var dynamo = utils.dynamo({
	table: argv.table,
	query: argv.query || config.query,
	key: argv.key || config.env[argv.env].aws_access_key_id,
	secret: argv.secret || config.env[argv.env].aws_secret_access_key,
	region: argv.region || config.env[argv.env].region,
	index: argv.index,
	rate: argv.rate || config.rate
});
function search(params, downloadLimit) {
	var msecPerItem = Math.round(10 / params.Limit / rate );
	params.Limit = downloadLimit;
	var method = params.KeyConditions ? dynamo.query : dynamo.scan;
	var read = function(start, done, params) {
		process.stdout.write("Start: " + start + "\n");
		process.stdout.write("Done: " + done + "\n");
		process.stdout.write("Params: " + JSON.stringify(params) + "\n");
		method.call(
			dynamo,
			params,
			function (err, data) {
				if (err != null) {
					throw err;
				}
				if (data == null) {
					throw 'dynamo returned NULL instead of data';
				}
				for (var idx = 0; idx < data.Items.length; idx++) {
					process.stdout.write(JSON.stringify(data.Items[idx]));
					process.stdout.write("\n");
				}
				var expected = start + msecPerItem * (done + data.Items.length);
				if (expected > Date.now()) {
					var sleepTime = expected - Date.now();
					process.stdout.write("Sleeping for " + sleepTime);
					sleep(expected - Date.now());
				}
				if (data.LastEvaluatedKey) {
					params.ExclusiveStartKey = data.LastEvaluatedKey;
					//read(start, done + data.Items.length, params);
				}
			}
		);
	};
	read(Date.now(), 0, params);
};

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
		process.stdout.write("Data: " + JSON.stringify(data) + "\n");
		var limit = rate;
		if(data.Table.ProvisionedThroughput.ReadCapacityUnits > 0 && data.Table.ProvisionedThroughput.ReadCapacityUnits < rate) {
			limit = data.Table.ProvisionedThroughput.ReadCapacityUnits;
		}
		var params = {
			TableName: argv.table,
			ReturnConsumedCapacity: 'NONE',
			Limit: data.Table.ProvisionedThroughput.ReadCapacityUnits > 0 ? data.Table.ProvisionedThroughput.ReadCapacityUnits : dynamo.rate
		};
		if (argv.index) {
			params.IndexName = argv.index
		}

		if (argv.query || config.query) {
			params.KeyConditions = JSON.parse(argv.query || config.query);
		}
		search(params, limit);
	}
);
