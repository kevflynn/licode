/*global require, exports, setInterval*/
'use strict';
var logger = require('./common/logger').logger;

// Logger
var log = logger.getLogger('EcCloudHandler');
var db = require('./database').db;
var _ = require('lodash');

const EA_TIMEOUT = 30000;
const EA_OLD_TIMEOUT = 30 * 60 * 1000;
const GET_EA_INTERVAL = 5000;
const AGENTS_ATTEMPTS = 5;
const AGENTS_GENERAL_QUEUE_ATTEMPTS = 2;
const WARN_UNAVAILABLE = 503;
const WARN_TIMEOUT = 504;
exports.EcCloudHandler = function (spec) {
  var that = {},
  amqper = spec.amqper,
  agents = {},
  timedOutErizos = [];

  that.getErizoAgents = function () {
    db.erizoJS
      .find({})
      .toArray((err, erizos) => {
        if (err) {
          log.error(`message: failed to fetch erizos from db, ${logger.objectToLog(err)}`);
          return;
        }

        const now = new Date();

        const groupedByAgent = _.groupBy(erizos, 'erizoAgentID');
        const counts = _.mapValues(groupedByAgent, (erizoJSs) => {
          const groupedByErizoJS = _.groupBy(erizoJSs, 'erizoJSID');
          const jsStats = _.mapValues(groupedByErizoJS, group =>
            _.minBy(group, stat => now - stat.lastUpdated)
          );
          // BEWARE! When unused agent is quickly destroyed and restarted, the old one might
          //         pass this filter and eventually be treated as the least used one,
          //         even though it doesn't exist anymore. Usually falling back to general
          //         ErizoAgent queue should do.
          const recentStats = _.pickBy(jsStats, stat => (now - stat.lastUpdated) <= EA_TIMEOUT);
          const recentStatsValues = _.values(recentStats);
          return _.reduce(
            recentStatsValues,
            (acc, { publishersCount, subscribersCount }) => {
              acc.publishersCount += publishersCount;
              acc.subscribersCount += subscribersCount;
              acc.count += 1;
              return acc;
            },
            { publishersCount: 0, subscribersCount: 0, count: 0 }
          );
        });
        agents = _.pickBy(counts, ({ count }) => count > 0);

        timedOutErizos = erizos.filter(({ lastUpdated }) => {
          const diff = now - lastUpdated;
          return diff > EA_TIMEOUT && diff <= EA_OLD_TIMEOUT;
        });

        const oldErizos = erizos.filter(({ lastUpdated }) => (now - lastUpdated) > EA_OLD_TIMEOUT);
        if (oldErizos.length) {
          deleteOldErizos(oldErizos);
        }
      });
  };

  that.getTimedOutErizos = function () {
    return timedOutErizos;
  };

  setInterval(that.getErizoAgents, GET_EA_INTERVAL);

  var deleteOldErizos = function (erizos) {
    var ids = erizos.map(({ _id }) => _id);
    db.erizoJS
      .remove({ _id: { $in: ids } }, function(error) {
        if (error) {
          log.warn('message: failed to remove old erizos, ' + logger.objectToLog(error));
        }
      });
  };

  let getErizoAgent;
  if (global.config.erizoController.cloudHandlerPolicy) {
    getErizoAgent = require(`./ch_policies/${global.config.erizoController.cloudHandlerPolicy}`).getErizoAgent;
  } else {
    getErizoAgent = (agents) => 'ErizoAgent';
  }

  const tryAgain = function (count, agentQueue, callback) {
    if (count >= AGENTS_ATTEMPTS) {
      callback('timeout');
      return;
    }

    let nextAgentQueue;
    if (AGENTS_ATTEMPTS - count <= AGENTS_GENERAL_QUEUE_ATTEMPTS) {
      nextAgentQueue = 'ErizoAgent';
    } else {
      nextAgentQueue = getErizoAgent(agents);
    }

    log.warn(`message: agent selected timed out trying again, code: ${WARN_TIMEOUT}, agentQueue: ${agentQueue}, nextAgentQueue: ${nextAgentQueue}`);

    amqper.callRpc(nextAgentQueue, 'createErizoJS', [], {
      callback(resp) {
        if (resp === 'timeout') {
          tryAgain(++count, agentQueue, callback);
        } else {
          const { erizoId, agentId } = resp;
          log.info(`message: createErizoJS/tryAgain success, erizoId: ${erizoId}, agentId: ${agentId}, count: ${count}`);
          callback(erizoId, agentId);
        }
      }
    });
  };

  that.getErizoJS = function (callback) {
    const agentQueue = getErizoAgent(agents);

    log.info(`message: createErizoJS, agentQueue: ${agentQueue}`);

    amqper.callRpc(agentQueue, 'createErizoJS', [], {
      callback(resp) {
        if (resp === 'timeout') {
          tryAgain(0, agentQueue, callback);
        } else {
          const { erizoId, agentId } = resp;
          log.info(`message: createErizoJS success, erizoId: ${erizoId}, agentId: ${agentId}`);
          callback(erizoId, agentId);
        }
      }
    });
  };

  that.deleteErizoJS = function(erizoId) {
    log.info ('message: deleting erizoJS, erizoId: ' + erizoId);
    amqper.broadcast('ErizoAgent', {method: 'deleteErizoJS', args: [erizoId]}, function(){});
  };

  return that;
};
