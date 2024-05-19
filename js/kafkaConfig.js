"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.readConfig = exports.confluent = void 0;
var dotenv_1 = require("dotenv");
var fs_1 = require("fs");
dotenv_1.default.config();
exports.confluent = {
    cloudConfig: {
        'bootstrap.servers': process.env.BOOTSTRAP_SERVERS || '',
        'sasl.username': process.env.SASL_USERNAME || '',
        'sasl.password': process.env.SASL_PASSWORD || '',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
    },
    topic: process.env.TOPIC || '',
};
var readConfig = function (fileName) {
    var data = fs_1.default.readFileSync(fileName, "utf8").toString().split("\n");
    return data.reduce(function (config, line) {
        var _a = line.split("="), key = _a[0], value = _a[1];
        if (key && value) {
            config[key] = value;
        }
        return config;
    }, {});
};
exports.readConfig = readConfig;
