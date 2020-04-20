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

var argv = utils.config({
    demand: ['table'],
    optional: ['rate', 'query', 'key', 'secret', 'region', 'index'],
    usage: 'Archives Dynamo DB table to standard output in JSON\n' +
           'Usage: dynamo-archive --table my-table [--rate 100] [--query "{}"] [--region us-east-1] [--key AK...AA] [--secret 7a...IG] [--index index-name]'
});

var dynamo = utils.dynamo(argv);
function search(params) {
    var msecPerItem = Math.round(1000 / params.Limit / ((argv.rate || 100) / 100));
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
		    process.stdout.write("No.of Items: " + data.Items.length + "\n");
		    process.stdout.write("Data: " + JSON.stringify(data) + "\n");
                for (var idx = 0; idx < data.Items.length; idx++) {

                    process.stdout.write(JSON.stringify(data.Items[idx]));
                    process.stdout.write("\n");
                }
                var expected = start + msecPerItem * (done + data.Items.length);
                if (expected > Date.now()) {
                    sleep(expected - Date.now());
                }
		    process.stdout.write("Last EvaluatedKey: " + data.LastEaluatedKey + "\n");
                if (data.LastEvaluatedKey) {
                    params.ExclusiveStartKey = data.LastEvaluatedKey;
                    read(start, done + data.Items.length, params);
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
        var params = {
            TableName: argv.table,
            ReturnConsumedCapacity: 'NONE',
            Limit: data.Table.ProvisionedThroughput.ReadCapacityUnits
        };
        if (argv.index) {
            params.IndexName = argv.index
        }

        if (argv.query) {
            params.KeyConditions = JSON.parse(argv.query);
        }
        search(params);
    }
);
