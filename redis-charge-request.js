"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const redis = require("redis");
const util = require("util");
const KEY_BALANCE_SUFFIX = `/balance`;
const KEY_CHARGE_SUFFIX = `/charge`;
const DEFAULT_CHARGE = 5;

// We will use only one connection for the whole session
// Each client connection initialization has a very high latency,
// this will strongly impact the latency of the chargeRequest method.
// By initializing the connection in advance, we can improve the execution time
// of this request to 1-digit ms, or even sub-ms
const redisClient = getRedisClient();

exports.chargeRequestRedis = async function (input) {
    let startTime = new Date();
    let key = input.key;
    let remainingBalance = await getBalanceRedis(redisClient, key);
    let charges = await getCharges(redisClient, key)
    const isAuthorized = authorizeRequest(remainingBalance, charges);
    if (!isAuthorized) {
        return {
            key,
            remainingBalance,
            isAuthorized,
            charges,
        };
    }
    remainingBalance = await chargeRedis(redisClient, key, charges);
    let endTime = new Date();

    console.log("Charge request latency is " + (endTime - startTime) + "ms");
    return {
        key,
        remainingBalance,
        charges,
        isAuthorized,
    };
};
exports.resetRedis = async function (input) {
    const ret = new Promise((resolve, reject) => {
        redisClient.set(input.key + KEY_BALANCE_SUFFIX, String(input.balance), (err, res) => {
            if (err) {
                reject(err);
            }
            else {
                resolve(input.balance);
            }
        });
        redisClient.set(input.key + KEY_CHARGE_SUFFIX, String(input.charge), (err, res) => {
            if (err) {
                reject(err);
            }
            else {
                resolve(input.balance);
            }
        });
    });
    return ret;
};

function getRedisClient() {
    try {
        const client = new redis.RedisClient({
            host: process.env.ENDPOINT,
            port: parseInt(process.env.PORT || "6379"),
        });
        client.on("ready", () => {
            console.log('redis client ready');
        });
        return client;
    }
    catch (error) {
        throw error;
    }
}

function disconnectRedis(client) {
    try {
        client.quit((error, res) => {
            if (error) {
                throw error;
            }
            else if (res === "OK") {
                console.log('redis client disconnected');
            }
            else {
                throw error("unknown error closing redis connection.");
            }
        });
    } catch (error) {
        throw error;
    }
}

function authorizeRequest(remainingBalance, charges) {
    return remainingBalance >= charges;
}

async function getCharges(redisClient, key) {
    const res = await util.promisify(redisClient.get).bind(redisClient).call(redisClient, key + KEY_CHARGE_SUFFIX);
    return parseInt(res || DEFAULT_CHARGE);
}

async function getBalanceRedis(redisClient, key) {
    const res = await util.promisify(redisClient.get).bind(redisClient).call(redisClient, key + KEY_BALANCE_SUFFIX);
    return parseInt(res || "0");
}
async function chargeRedis(redisClient, key, charges) {
    return util.promisify(redisClient.decrby).bind(redisClient).call(redisClient, key + KEY_BALANCE_SUFFIX, charges);
}

// To close connection to Redis before shutting down
process.on('exit', function () {
    // close connection before shutting down
    disconnectRedis(redisClient);
});
