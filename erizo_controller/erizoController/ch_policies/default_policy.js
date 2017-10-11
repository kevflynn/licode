const _ = require('lodash');

exports.getErizoAgent = function (agents) {
    const pairs = _.toPairs(agents);
    const leastConnectionsAgent = _.minBy(pairs,
        ([agentId, { publishersCount, subscribersCount }]) => publishersCount + subscribersCount);

    if (!leastConnectionsAgent) {
        return 'ErizoAgent';
    }

    return `ErizoAgent_${leastConnectionsAgent[0]}`;
};
