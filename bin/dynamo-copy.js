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

var configData = fs.readFileSync('config.json');
var config = JSON.parse(configData);

var argv = utils.config({
    demand: ['srctable', 'desttable'],
    optional: ['rate', 'query', 'srckey', 'srcsecret', 'srcregion', 'index', 'destkey', 'destsecret', 'destregion', 'srcenv', 'destenv'],
    usage: 'Copy Dynamo DB tables from one AWS account to another AWS account\n' +
           'Usage: dynamo-copy --srctable src-table --desttable dest-table  [--rate 100] [--query "{}"] [--srcregion us-east-1] [--srckey AK...AA] [--srcsecret 7a...IG] [--index index-name] [--destregion us-east-1] [--destkey AK...AA] [--destsecret 7a...IG] \n\n' + 
           'Usage: dynamo-copy --srctable src-table --desttable dest-table [--srcenv sourcenv] [--destenv destenv] \n\n'
});

var srcdynamo = utils.dynamo({
			table: argv.srctable,
			query: argv.query || config.query,
			key: argv.srckey || config.env[argv.srcenv].aws_access_key_id,
			secret: argv.srcsecret || config.env[argv.srcenv].aws_secret_access_key,
			region: argv.srcregion || config.env[argv.srcenv].region,
			index: argv.index 
			rate: argv.rate || config.rate
		});

var destdynamo = utils.dynamo({
			table: argv.desttable,
			key: argv.destkey || config.env[argv.destenv].aws_access_key_id,
			secret: argv.destsecret || config.env[argv.destenv].aws_secret_access_key,
			region: argv.destregion|| config.env[argv.destenv].region,
			rate: argv.rate || config.rate
		});

var destQuota = 0;
var destStart = Date.now();
var destSecPerItem = 0;
var destDone = 0;

destdynamo.describeTable(
    {
        TableName: argv.desttable
    },
    function (err, data) {
        if (err != null) {
            throw err;
        }
        if (data == null) {
            throw 'Table ' + argv.desttable + ' not found in DynamoDB';
        }
        destQuota = data.Table.ProvisionedThroughput.WriteCapacityUnits;
        destStart = Date.now();
        destSecPerItem = Math.round(1000 / destQuota / ((argv.rate || 100) / 100));
        destDone = 0;

	var params = {
		quota: data.Table.ProvisionedThroughput.WriteCapacityUnits,
		msecPerItem: Math.round(1000 / destQuota / ((argv.rate || 100) / 100))
	};
	getSourceTable(params);
    }
);

function getSourceTable(destParams) {
	srcdynamo.describeTable(
	    {
	        TableName: argv.srctable
	    },
	    function (err, data) {
	        if (err != null) {
	            throw err;
	        }
	        if (data == null) {
	            throw 'Table ' + argv.srctable + ' not found in DynamoDB';
	        }
	        var srcParams = {
	            TableName: argv.srctable,
	            ReturnConsumedCapacity: 'NONE',
	            Limit: data.Table.ProvisionedThroughput.ReadCapacityUnits
	        };
	        if (argv.index) {
	            srcParams.IndexName = argv.index
	        }
	
	        if (argv.query) {
	            srcParams.KeyConditions = JSON.parse(argv.query);
	        }
	        search(srcParams, destParams);
	    }
	);
}

function search(srcParams, destParams) {
    var msecPerItem = Math.round(1000 / srcParams.Limit / ((argv.rate || 100) / 100));
    var method = srcParams.KeyConditions ? srcdynamo.query : srcdynamo.scan;
    var read = function(start, done, srcParams, destParams) {
	process.stdout.write("Start: " + start + ", Done: " + Done + "\n");
        method.call(
            srcdynamo,
            srcParams,
            function (err, data) {
                if (err != null) {
                    throw err;
                }
                if (data == null) {
                    throw 'dynamo returned NULL instead of data';
                }

                for (var idx = 0; idx < data.Items.length; idx++) {
			destdynamo.putItem(
	                    {
	                        TableName: argv.desttable,
	                        Item: data.Items[idx]
	                    },
	                    function (err, data) {
	                        if (err) {
	                            console.log(err, err.stack);
	                            throw err;
	                        }
	                    }
	                );
	                ++done;
        	        var expected = start + destParams.msecPerItem * done;
                	if (expected > Date.now()) {
	                    sleep(expected - Date.now());
        	        }

                    process.stdout.write(JSON.stringify(data.Items[idx]));
                    process.stdout.write("\n");
                }
                var srcexpected = start + srcParams.msecPerItem * done;
                if (srcexpected > Date.now()) {
                    sleep(srcexpected - Date.now());
                }
                if (data.LastEvaluatedKey) {
                    srcParams.ExclusiveStartKey = data.LastEvaluatedKey;
                    read(start, done, srcParams, destParams);
                }
            }
        );
    };
    read(Date.now(), 0, srcParams, destParams);
};

