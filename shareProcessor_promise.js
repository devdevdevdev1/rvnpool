
var redis = require('redis');
var Stratum = require('stratum-pool');
const loggerFactory = require('./logger.js');
const logger = loggerFactory.getLogger('ShareProcessor', 'system');

module.exports = function(poolConfig) {
	var redisConfig = poolConfig.redis;
	var coin = poolConfig.coin.name;
	var forkId = process.env.forkId;
	let logger = loggerFactory.getLogger(`ShareProcessor [:${forkId}]`, coin);
	var logSystem = 'Pool';
	var logComponent = coin;
	var logSubCat = 'Thread ' + (parseInt(forkId) + 1);
	var connection = redis.createClient(redisConfig.port, redisConfig.host);
	connection.on('ready', function() {
		logger.debug( `{"message:" Share processing setup with redis (${redisConfig.host}:${redisConfig.port})}`);
	});
	connection.on('error', function(err) {
		logger.error(`{"message": "Redis client had an error", "data": ${JSON.stringify(err)}}`);
	});
	connection.on('end', function() {
		logger.error(`{"message": "Connection to redis database has been ended"}`);
	});
	connection.info(function(error, response) {
		if (error) {
			logger.error(`{"message": "Redis version check failed"}`);
			return;
		}
		var parts = response.split('\r\n');
		var version;
		var versionString;
		for (var i = 0; i < parts.length; i++) {
			if (parts[i].indexOf(':') !== -1) {
				var valParts = parts[i].split(':');
				if (valParts[0] === 'redis_version') {
					versionString = valParts[1];
					version = parseFloat(versionString);
					break;
				}
			}
		}
		if (!version) {
			logger.error(`{"message": "Could not detect redis version - may be super old or broken"}`);
		}
		else if (version < 2.6) {
			logger.error(`{"message": "You're using redis version ${versionString} the minimum required version is 2.6. Follow the damn usage instructions..."}`);
		}
	});

	
	this.handleShare = async function(isValidShare, isValidBlock, shareData) {
		var redisCommands = [];
		if (isValidShare) {
			redisCommands.push(['hincrbyfloat', coin + ':shares:roundCurrent', shareData.worker, shareData.difficulty]);
			redisCommands.push(['hincrby', coin + ':stats', 'validShares', 1]);
		} else {
			redisCommands.push(['hincrby', coin + ':stats', 'invalidShares', 1]);
		}
		var dateNow = Date.now();
		var hashrateData = [isValidShare ? shareData.difficulty : -shareData.difficulty, shareData.worker, dateNow];
		redisCommands.push(['zadd', coin + ':hashrate', dateNow / 1000 | 0, hashrateData.join(':')]);

		if (isValidBlock) {
			var blockEffort = parseFloat(0);

			await this.getCurrentRoundShares(function (roundShares) {
					logger.debug("calling GetCurrentRoundShares");
					blockEffort = parseFloat([roundShares / shareData.blockDiff]);
					logger.debug(`{"message": "Calculating Block Effort", "totalRoundShares": "${roundShares}", "blockEffort": "${blockEffort}"}`);
				});


			redisCommands.push(['rename', coin + ':shares:roundCurrent', coin + ':shares:round' + shareData.height]);
			redisCommands.push(['rename', coin + ':shares:timesCurrent', coin + ':shares:times' + shareData.height]);
			redisCommands.push(['sadd', coin + ':blocksPending', [shareData.blockHash, shareData.txHash, shareData.height].join(':')]);
			redisCommands.push(['sadd', coin + ':blocksExplorer', [dateNow, shareData.height, shareData.blockHash, shareData.worker, blockEffort].join(':')]);
			redisCommands.push(['zadd', coin + ':lastBlock', dateNow / 1000 | 0, [shareData.blockHash, shareData.txHash, shareData.worker, shareData.height, dateNow].join(':')]);
			redisCommands.push(['zadd', coin + ':lastBlockTime', dateNow / 1000 | 0, [dateNow].join(':')]);
			redisCommands.push(['hincrby', coin + ':stats', 'validBlocks', 1]);
			redisCommands.push(['hincrby', coin + ':blocksFound', shareData.worker, 1]);
			
		}
		else if (shareData.blockHash) {            
			redisCommands.push(['hincrby', coin + ':stats', 'invalidBlocks', 1]);           
		}
		connection.multi(redisCommands).exec(function(err, replies) {
			logger.debug("Sent all data to redis")
			if (err)
			logger.error(`{"message": "Error with share processor multi", "data": ${JSON.stringify(err)}}`);
		});
	};


	this.getCurrentRoundShares = function(cback) {
		let processHasFinishedSuccessfully = true;
		return new Promise((resolve, reject) => {
			connection.hgetall('ravencoin:shares:roundCurrent', function(error,result) {
				if (error) {
					logger.error(`{"message": "Error getCurrentRoundShares", "data": "${error}"}`);
					processHasFinishedSuccessfully = false;
					cback(error);
					return;
				} else {
					logger.debug(`{"message": "Calculating all shares in current round"}`);
					logger.debug(result.toString());
	
					var _shareTotal = parseFloat(0);
					for (var worker in result) {
						logger.debug(`{"message": "Shares for each Worker", "worker": "${worker}", "shares": "${parseFloat(result[worker])}"} }`);
						_shareTotal += parseFloat(result[worker]);
					}
					logger.debug("Total Shares: " + _shareTotal );
					cback(_shareTotal);
				}
			});
			if (processHasFinishedSuccessfully){
			   resolve();
			}
			else {
			   reject(Error('Failed'));
			}
		});
	}
};
