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
    demand: ['srctable', 'desttable'],
    optional: ['rate', 'query', 'srckey', 'srcsecret', 'srcregion', 'index', 'destkey', 'destsecret', 'destregion', 'srcenv', 'destenv', 'debug', 'lastkey'],
    usage: 'Copy Dynamo DB tables from one AWS account to another AWS account\n' +
           'Usage: dynamo-copy --srctable src-table --desttable dest-table [--srcenv sourcenv] [--destenv destenv] [--debug level] [--lastkey LastEvaluatedKey]\n\n' + 
	   'debug level 1-99, 1 - quiet mode, 99 - verbose log\n'
});

var configData = fs.readFileSync('config.json');
var config = JSON.parse(configData);

var rate = argv.rate || config.rate || 100;
rate = rate > 0 ? rate : 1;
var debug = argv.debug || 20;
var quota = config.quota || 100;
utils.setLogLevel(debug);

var srcenv = argv.srcenv || config.srcenv;
var destenv = argv.destenv || config.destenv;

var srcdynamo = utils.dynamo({
			table: argv.srctable,
			query: argv.query || config.query,
			key: argv.srckey || config.env[srcenv].aws_access_key_id,
			secret: argv.srcsecret || config.env[srcenv].aws_secret_access_key,
			region: argv.srcregion || config.env[srcenv].region,
			index: argv.index,
			endpointUrl: config.env[srcenv].endpointUrl,
			rate: rate
		});

var destdynamo = utils.dynamo({
			table: argv.desttable,
			key: argv.destkey || config.env[destenv].aws_access_key_id,
			secret: argv.destsecret || config.env[destenv].aws_secret_access_key,
			region: argv.destregion|| config.env[destenv].region,
			endpointUrl: config.env[destenv].endpointUrl,
			rate: rate
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
	utils.log(10, "Destination table Quota: " + destQuota);
	destQuota = destQuota == 0 || destQuota > quota ? quota : destQuota;
	utils.log(10, "Using Quota: " + destQuota);
        destStart = Date.now();
        destSecPerItem = 1000 / destQuota;
	utils.log(10, "Destination table - No.of milliseconds to process one item: " + destSecPerItem);
	utils.log(10, "No.of items to process per iteration: " + rate);
        destDone = 0;

	var params = {
		quota: destQuota,
		msecPerItem: destSecPerItem
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
		var srcQuota = data.Table.ProvisionedThroughput.ReadCapacityUnits;
		srcQuota = srcQuota == 0 || srcQuota > quota ? quota : srcQuota;
                var srcSecPerItem = 1000 / srcQuota;
	        var srcParams = {
	            TableName: argv.srctable,
	            ReturnConsumedCapacity: 'NONE',
	            Limit: data.Table.ProvisionedThroughput.ReadCapacityUnits > 0 ? data.Table.ProvisionedThroughput.ReadCapacityUnits : rate
	        };
		var srcSettings = {
			quota: srcQuota,
			msecPerItem: srcSecPerItem
		}
		utils.log(10, "Source Table AWS Limit: " + data.Table.ProvisionedThroughput.ReadCapacityUnits);
		utils.log(10, "Source Table Download Limit: " + srcParams.Limit);
    		utils.log(10, "Source table items - No.of milliseconds to process one item: " + srcSettings.msecPerItem);
	        if (argv.index) {
	            srcParams.IndexName = argv.index
	        }
	        if (argv.query) {
	            srcParams.KeyConditions = JSON.parse(argv.query);
	        }
	        search(srcParams, srcSettings, destParams);
	    }
	);
}

function search(srcParams, srcSettings, destParams) {
    var method = srcParams.KeyConditions ? srcdynamo.query : srcdynamo.scan;
    var read = function(start, done, srcParams, destParams) {
    utils.log(20, "\nDate: " + new Date(Date.now()) + ", Items Copied: " + done);
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
			if(destParams.msecPerItem > 0) {
        	       		var expected = start + destParams.msecPerItem * done;
               		 	if (expected > Date.now()) {
		                    sleep(expected - Date.now());
	        	        }
			}
                    	utils.log(80, JSON.stringify(data.Items[idx]) + "\n");
                }
		if(srcSettings.msecPerItem > 0) {
        	        var srcexpected = start + srcSettings.msecPerItem * done;
                	if (srcexpected > Date.now()) {
               	     		sleep(srcexpected - Date.now());
               		}
		}

                if (data.LastEvaluatedKey) {
                    srcParams.ExclusiveStartKey = data.LastEvaluatedKey;
		    utils.log(20, "LastEvaluatedKey: " + JSON.stringify(srcParams.ExclusiveStartKey));
                    read(start, done, srcParams, destParams);
                }
            }
        );
    };
    if(argv.lastkey || config.lastkey) {
	    srcParams.ExclusiveStartKey = JSON.parse(argv.lastkey || config.lastkey);
    }
    read(Date.now(), 0, srcParams, destParams);
};

