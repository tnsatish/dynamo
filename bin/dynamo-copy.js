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
    demand: ['srctable', 'desttable'],
    optional: ['rate', 'query', 'srckey', 'srcsecret', 'srcregion', 'index', 'destkey', 'destsecret', 'destregion'],
    usage: 'Copy Dynamo DB tables from one AWS account to another AWS account\n' +
           'Usage: dynamo-copy --srctable src-table --desttable dest-table  [--rate 100] [--query "{}"] [--srcregion us-east-1] [--srckey AK...AA] [--srcsecret 7a...IG] [--index index-name] [--destregion us-east-1] [--destkey AK...AA] [--destsecret 7a...IG]'
});

var srcdynamo = utils.dynamo({
			table: argv.srctable,
			query: argv.query,
			key: argv.srckey,
			secret: argv.srcsecret,
			region: argv.srcregion,
			index: argv.index,
			rate: argv.rate
		});

var destdynamo = utils.dynamo({
			table: argv.desttable,
			key: argv.destkey,
			secret: argv.destsecret,
			region: argv.destregion,
			rate: argv.rate
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
            throw 'Table ' + argv.table + ' not found in DynamoDB';
        }
        destQuota = data.Table.ProvisionedThroughput.WriteCapacityUnits;
        destStart = Date.now();
        destSecPerItem = Math.round(1000 / destQuota / ((argv.rate || 100) / 100));
        destDone = 0;
	console.log("Data in describeTable event");
	console.log(destQuota);
	console.log(destStart);
	console.log(destSecPerItem);
	console.log(destDone);

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
	        //process.stdout.write("Data: " + JSON.stringify(data) + "\n");
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
	    process.stdout.write("Start: " + start + "\n");
	    process.stdout.write("Done: " + done + "\n");
	    process.stdout.write("Params: " + JSON.stringify(srcParams) + "\n");
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

		    process.stdout.write("No.of Items: " + data.Items.length + "\n");
		    process.stdout.write("Data: " + JSON.stringify(data) + "\n");

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
		    process.stdout.write("Last EvaluatedKey: " + data.LastEaluatedKey + "\n");
                if (data.LastEvaluatedKey) {
                    srcParams.ExclusiveStartKey = data.LastEvaluatedKey;
                    read(start, done, srcParams, destParams);
                }
            }
        );
    };
    read(Date.now(), 0, srcParams, destParams);
};



/*
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
        var quota = data.Table.ProvisionedThroughput.WriteCapacityUnits;
        var start = Date.now();
        var msecPerItem = Math.round(1000 / quota / ((argv.rate || 100) / 100));
        var done = 0;
        readline.createInterface(process.stdin, process.stdout).on(
            'line',
            function(line) {
                dynamo.putItem(
                    {
                        TableName: argv.table,
                        Item: JSON.parse(line)
                    },
                    function (err, data) {
                        if (err) {
                            console.log(err, err.stack);
                            throw err;
                        }
                    }
                );
                ++done;
                var expected = start + msecPerItem * done;
                if (expected > Date.now()) {
                    sleep(expected - Date.now());
                }
            }
        );
    }
);

function search(params) {
    var msecPerItem = Math.round(1000 / params.Limit / ((argv.rate || 100) / 100));
    var method = params.KeyConditions ? srcdynamo.query : srcdynamo.scan;
    var read = function(start, done, params) {
	    process.stdout.write("Start: " + start + "\n");
	    process.stdout.write("Done: " + done + "\n");
	    process.stdout.write("Params: " + JSON.stringify(params) + "\n");
        method.call(
            srcdynamo,
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

srcdynamo.describeTable(
    {
        TableName: argv.table
    },
    function (err, data) {
        if (err != null) {
            throw err;
        }
        if (data == null) {
            throw 'Table ' + argv.srctable + ' not found in DynamoDB';
        }
        process.stdout.write("Data: " + JSON.stringify(data) + "\n");
        var params = {
            TableName: argv.srctable,
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

*/
