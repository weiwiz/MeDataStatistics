'use strict';
var util = require('util');
var _ = require('lodash');
var async = require('async');
var schedule = require('node-schedule');
var mongoose = require('mongoose');
var VirtualDevice = require('./../virtual-device').VirtualDevice;
var logger = require('../mlogger/mlogger.js');
var ONE_HOUR_MS = 1000 * 60 * 60;
var ONE_DAY_MS = ONE_HOUR_MS * 24;
var REPORT_DATA_TYPE = ["dailyReport", "monthlyReport", "yearlyReport", "allReport"];
var DEVICE_DATA_SCHEMA = {
    "uuid": mongoose.SchemaTypes.String,
    "userId": mongoose.SchemaTypes.String,
    "type": mongoose.SchemaTypes.String,
    "timestamp": mongoose.SchemaTypes.Date,
    "offset": mongoose.SchemaTypes.Number,
    "data": mongoose.SchemaTypes.Mixed
};
var OPERATION_SCHEMAS = {
    statistics: {
        "type": "object",
        "properties": {
            "dataType": {"type": "string"},
            "timestamp": {
                "type": "object",
                "properties": {
                    "$gte": {"type": "string"},
                    "$lt": {"type": "string"}
                }
            }
        }
    }
};

var dataMap = function () {
    emit(this.uuid, this);
};
var dailyDataReduce = function (key, values) {
    var record = JSON.parse(JSON.stringify(values[0]));
    delete record.__v;
    delete record._id;
    if ("050608070001" === record.type) {
        var sumTemp = 0;
        values.forEach(function (item) {
            sumTemp += item.data.dis_temp;
        });
        record.data.dis_temp = sumTemp / values.length;
    }
    else if ("040B08040004" === record.type) {
        var sumPower = 0;
        values.forEach(function (item) {
            sumPower += item.data.power;
        });
        record.data.power = sumPower / values.length;
        if (values.length > 1) {
            record.data.energyUsed = values[values.length - 1].data.energyUsed - values[0].data.energyUsed;
            record.data.energySaved = values[values.length - 1].data.energySaved - values[0].data.energySaved;
        }
    }
    else if ("040B01000001" === record.type) {
        var sumEffectiveVolt = 0;
        var sumEffectiveCurrent = 0;
        values.forEach(function (item) {
            if (16 === item.data.direct) {
                sumEffectiveVolt += item.data.effectiveVolt;
                sumEffectiveCurrent += item.data.effectiveCurrent;
            }
            else if (17 === item.data.direct) {
                sumEffectiveVolt -= item.data.effectiveVolt;
                sumEffectiveCurrent -= item.data.effectiveCurrent;
            }
        });
        record.data.power = (sumEffectiveVolt / values.length) * (sumEffectiveCurrent / values.length) / 100;
        delete record.data.effectiveVolt;
        delete record.data.effectiveCurrent;
        delete record.data.direct;
        if (values.length > 1) {
            record.data.toGrid = values[values.length - 1].data.toGrid - values[0].data.toGrid;
            record.data.toUser = values[values.length - 1].data.toUser - values[0].data.toUser;
        }
    }
    else if ("040B01000005" === record.type) {
        var sumPacToGrid = 0;
        var sumPacToUser = 0;
        values.forEach(function (item) {
            sumPacToGrid += item.data.pacToGrid;
            sumPacToUser += item.data.pacToUser;
        });
        record.data.pacToGrid = sumPacToGrid / values.length;
        record.data.pacToUser = sumPacToUser / values.length;
        if (values.length > 1) {
            record.data.eDisChargeTotal = values[values.length - 1].data.eDisChargeTotal - values[0].data.eDisChargeTotal;
            record.data.eChargeTotal = values[values.length - 1].data.eChargeTotal - values[0].data.eChargeTotal;
            record.data.eToGridTotal = values[values.length - 1].data.eToGridTotal - values[0].data.eToGridTotal;
        }
    }
    else if ("040B01000004" === record.type) {
        var sumCurrentPower = 0;
        values.forEach(function (item) {
            sumCurrentPower += item.data.currentPower;
        });
        record.data.currentPower = sumCurrentPower / values.length;
        if (values.length > 1) {
            record.data.totalEnergy = values[values.length - 1].data.totalEnergy - values[0].data.totalEnergy;
        }
    }
    return record;
};
var dataReduce = function (key, values) {
    var record = JSON.parse(JSON.stringify(values[0]));
    delete record.__v;
    delete record._id;
    if ("050608070001" === record.type) {
        var sumTemp = 0;
        values.forEach(function (item) {
            sumTemp += item.data.dis_temp
        });
        record.data.dis_temp = sumTemp / values.length;
    }
    else if ("040B08040004" === record.type) {
        var sumPower = 0;
        var sumEnergyUsed = 0;
        var sumEnergySaved = 0;
        values.forEach(function (item) {
            sumPower += item.data.power;
            sumEnergyUsed += item.data.energyUsed;
            sumEnergySaved += item.data.energySaved
        });
        record.data.power = sumPower / values.length;
        record.data.energyUsed = sumEnergyUsed;
        record.data.energySaved = sumEnergySaved;
    }
    else if ("040B01000001" === record.type) {
        var sumCurPower = 0;
        var sumToGrid = 0;
        var sumToUser = 0;
        values.forEach(function (item) {
            sumToGrid += item.data.toGrid;
            sumToUser += item.data.toUser;
            sumCurPower += item.data.power;
        });
        record.data.power = sumCurPower / values.length;
        record.data.toGrid = sumToGrid;
        record.data.toUser = sumToUser;
    }
    else if ("040B01000005" === record.type) {
        var sumPacToGrid = 0;
        var sumPacToUser = 0;
        var sumEDisChargeTotal = 0;
        var sumEChargeTotal = 0;
        var sumEToGridTotal = 0;
        values.forEach(function (item) {
            sumPacToGrid += item.data.pacToGrid;
            sumPacToUser += item.data.pacToUser;
            sumEDisChargeTotal += item.data.eDisChargeTotal;
            sumEChargeTotal += item.data.eChargeTotal;
            sumEToGridTotal += item.data.eToGridTotal;
        });
        record.data.pacToGrid = sumPacToGrid / values.length;
        record.data.pacToUser = sumPacToUser / values.length;
        record.data.eDisChargeTotal = sumEDisChargeTotal;
        record.data.eChargeTotal = sumEChargeTotal;
        record.data.eToGridTotal = sumEToGridTotal;
    }
    else if ("040B01000004" === record.type) {
        var sumCurrentPower = 0;
        var sumTotalEnergy = 0;
        values.forEach(function (item) {
            sumCurrentPower += item.data.currentPower;
            sumTotalEnergy += item.data.totalEnergy;
        });
        record.data.currentPower = sumCurrentPower / values.length;
        record.data.totalEnergy = sumTotalEnergy;
    }

    return record;
};
/**
 * @constructor
 * */
function dataStatistics(conx, uuid, token, configurator) {
    this.db = null;
    this.scheduleJob = null;
    this.dataModel = null;
    this.dailyDataModel = null;
    this.monthlyDataModel = null;
    this.yearlyDataModel = null;
    this.allDataModel = null;
    this.statisticsAll = function () {
        var self = this;
        async.mapSeries(REPORT_DATA_TYPE, function (dataType, callback) {
            var curTime = Date.now();
            var curDate = new Date(curTime);
            var startTime = null;
            var endTime = null;
            if ("dailyReport" === dataType) {
                endTime = curTime - curTime % ONE_HOUR_MS;
                startTime = endTime - ONE_HOUR_MS;
            }
            else if ("monthlyReport" === dataType) {
                startTime = curDate.setHours(0, 0, 0, 0);
                endTime = startTime + ONE_DAY_MS;
            }
            else if ("yearlyReport" === dataType) {
                var curMoth = curDate.getMonth();
                curDate.setHours(0, 0, 0, 0);
                startTime = curDate.setMonth(curMoth, 1);
                endTime = curDate.setMonth(curMoth + 1, 1);
            }
            else if ("allReport" === dataType) {
                var curYear = curDate.getFullYear();
                curDate.setHours(0, 0, 0, 0);
                curDate.setMonth(0, 1);
                startTime = curDate.setFullYear(curYear);
                endTime = curDate.setFullYear(curYear + 1);
            }
            self.statistics({
                "dataType": dataType,
                "timestamp": {
                    "$gte": new Date(startTime).toISOString(),
                    "$lt": new Date(endTime).toISOString()
                }
            }, function (resp) {
                callback(null, resp.data);
            });
        }, function (error, results) {
            logger.debug(results);
        });
    };
    VirtualDevice.call(this, conx, uuid, token, configurator);
}
util.inherits(dataStatistics, VirtualDevice);

/**
 * 设备管理器初始化，将系统已经添加的设备实例化并挂载到Meshblu网络
 * */
dataStatistics.prototype.init = function () {
    var self = this;
    var db = mongoose.createConnection(self.configurator.getConf("meshblu_server.db_url"));
    mongoose.Promise = global.Promise;
    db.once('error', function (error) {
        logger.error(200005, error);
    });

    db.once('open', function () {
        self.db = db;
        self.dataModel = db.model("data", DEVICE_DATA_SCHEMA);
        self.dailyDataModel = db.model("daily_data", DEVICE_DATA_SCHEMA);
        self.monthlyDataModel = db.model("monthly_data", DEVICE_DATA_SCHEMA);
        self.yearlyDataModel = db.model("yearly_data", DEVICE_DATA_SCHEMA);
        self.allDataModel = self.db.model("all_data", DEVICE_DATA_SCHEMA);
        self.statisticsAll();
       /* self.scheduleJob = schedule.scheduleJob("3 * * * *", function () {
            self.statisticsAll();
        });*/
    });
};

/**
 * 远程RPC回调函数
 * @callback onMessage~statistics
 * @param {object} response:
 * {
 *      "payload":
 *      {
 *          "code":{number},
 *          "message":{string},
 *          "data":null
 *      }
 * }
 */
/**
 * 获取分析结果
 * @param {object} message:消息体
 * @param {onMessage~statistics} peerCallback: 远程RPC回调函数
 * */
dataStatistics.prototype.statistics = function (message, peerCallback) {
    var self = this;
    var responseMessage = {retCode: 200, description: "Success.", data: {}};
    self.messageValidate(message, OPERATION_SCHEMAS.statistics, function (error) {
        if (error) {
            responseMessage = error;
            peerCallback(error);
        }
        else {
            var srcDataModel = null;
            var disDataModel = null;
            var reduceFun = dataReduce;
            if ("dailyReport" === message.dataType) {
                srcDataModel = self.dataModel;
                disDataModel = self.dailyDataModel;
                reduceFun = dailyDataReduce;
            }
            else if ("monthlyReport" === message.dataType) {
                srcDataModel = self.dailyDataModel;
                disDataModel = self.monthlyDataModel;
            }
            else if ("yearlyReport" === message.dataType) {
                srcDataModel = self.monthlyDataModel;
                disDataModel = self.yearlyDataModel;
            }
            else if ("allReport" === message.dataType) {
                srcDataModel = self.yearlyDataModel;
                disDataModel = self.allDataModel;
            }
            else {
                responseMessage.retCode = 200001;
                responseMessage.message = "invalid report data type:" + message.dataType;
                peerCallback(responseMessage);
                return;
            }
            var command = {
                query: {
                    "timestamp": {
                        "$gte": new Date(message.timestamp.$gte),
                        "$lt": new Date(message.timestamp.$lt)
                    }
                },
                map: dataMap,
                reduce: reduceFun,
                verbose: true
            };
            srcDataModel.mapReduce(command, function (err, dbres, stats) {
                if (err) {
                    logger.debug(err);
                    responseMessage.retCode = 200000;
                    responseMessage.message = err.toString();
                    peerCallback(responseMessage);
                }
                else {
                    //logger.debug(dbres);
                    async.mapSeries(dbres, function (result, callback) {
                        delete result.value.__v;
                        delete result.value._id;
                        result.value.timestamp = command.query.timestamp.$gte;
                        var query = {
                            "uuid": result.value.uuid,
                            "timestamp": result.value.timestamp
                        };
                        var update = result.value;
                        var options = {
                            new: true,
                            upsert: true
                        };
                        disDataModel.findOneAndUpdate(query, update, options, function (error, doc) {
                            if (error) {
                                logger.debug(error)
                            }
                            else {
                                //logger.debug(doc)
                            }
                            callback(null,doc);
                        });
                    }, function (error, results) {
                        responseMessage.data = {statistics:message,stats:stats, results:results};
                        peerCallback(responseMessage);
                    });
                }
            });
        }
    });
};


module.exports = {
    Service: dataStatistics,
    OperationSchemas: OPERATION_SCHEMAS
};