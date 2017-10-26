'use strict';
var util = require('util');
var _ = require('lodash');
var schedule = require('node-schedule');
var mongoose = require('mongoose');
var VirtualDevice = require('./../virtual-device').VirtualDevice;
var logger = require('../mlogger/mlogger.js');
var ONE_HOUR_MS = 1000 * 60 * 60;
var ONE_DAY_MS = ONE_HOUR_MS * 24;
var REPORT_DATA_TYPE = ["allReport"];
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
var dataReduce = function (key, values) {
    var record = values[0];
    if ("050608070001" === record.type) {
        var sumTemp = 0;
        values.forEach(function (item) {
            sumTemp += item.data.dis_temp
        });
        record.data.dis_temp = sumTemp / values.length;
        delete record.__v;
    }
    else if ("040B08040004" === record.type) {
        var sumPower = 0;
        values.forEach(function (item) {
            sumPower += item.data.power
        });
        record.data.power = sumPower / values.length;
        record.data.energyUsed = values[values.length - 1].data.energyUsed - values[0].data.energyUsed;
        record.data.energySaved = values[values.length - 1].data.energySaved - values[0].data.energySaved;
        delete record.__v;
    }
    else if ("040B01000001" === record.type) {
        var sumEffectiveVolt = 0;
        var sumEffectiveCurrent = 0;
        var sumCurPower = 0;
        values.forEach(function (item) {
            if (item.data.power !== null && item.data.power !== undefined) {
                sumCurPower += tem.data.power;
            }
            else {
                if (16 === item.data.direct) {
                    sumEffectiveVolt += item.data.effectiveVolt;
                    sumEffectiveCurrent += item.data.effectiveCurrent;
                }
                else if (17 === item.data.direct) {
                    sumEffectiveVolt -= item.data.effectiveVolt;
                    sumEffectiveCurrent -= item.data.effectiveCurrent;
                }
            }
        });
        if (record.data.power !== null && record.data.power !== undefined) {
            record.data.power = sumCurPower / values.length;
        }
        else{
            record.data.power = (sumEffectiveVolt / values.length) * (sumEffectiveCurrent / values.length) % 100;
            delete record.data.effectiveVolt;
            delete record.data.effectiveCurrent;
            delete record.data.direct;
        }
        record.data.toGrid = values[values.length - 1].data.toGrid - values[0].data.toGrid;
        record.data.toUser = values[values.length - 1].data.toUser - values[0].data.toUser;
        delete record.__v;
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
        record.data.eDisChargeTotal = values[values.length - 1].data.eDisChargeTotal - values[0].data.eDisChargeTotal;
        record.data.eChargeTotal = values[values.length - 1].data.eChargeTotal - values[0].data.eChargeTotal;
        record.data.eToGridTotal = values[values.length - 1].data.eToGridTotal - values[0].data.eToGridTotal;
        delete record.__v;
    }
    else if ("040B01000004" === record.type) {
        var sumCurrentPower = 0;
        values.forEach(function (item) {
            sumCurrentPower += item.data.currentPower;
        });
        record.data.currentPower = sumCurrentPower / values.length;
        record.data.totalEnergy = values[values.length - 1].data.totalEnergy - values[0].data.totalEnergy;
        delete record.__v;
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
    this.statisticsAll = function () {
        var self = this;
        _.forEach(REPORT_DATA_TYPE, function (dataType) {
            var curTime = Date.now();
            var curDate = new Date(curTime);
            var startTime = null;
            var endTime = null;
            if ("dailyReport" === dataType) {
                endTime = curTime - curTime % ONE_HOUR_MS;
                startTime = endTime - ONE_HOUR_MS;

            }
            else if ("monthlyReport" === dataType) {
                startTime = curTime - curTime % ONE_DAY_MS;
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
                logger.debug(resp.data);
            });
        });


        //self.reportStatistics("050608070001", "dailyReport");

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
        self.statisticsAll();
        //self.scheduleJob = schedule.scheduleJob("0,*,*,*,*", self.statistics());
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
            if ("dailyReport" === message.dataType) {
                srcDataModel = self.dataModel;
                disDataModel = self.dailyDataModel;
            }
            else if ("monthlyReport" === message.dataType) {
                srcDataModel = self.dailyDataModel;
                disDataModel = self.monthlyDataModel;
            }
            else if ("yearlyReport" === message.dataType) {
                srcDataModel = self.monthlyDataModel;
                disDataModel = self.yearlyDataModel;
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
                reduce: dataReduce,
                verbose: true
            };
            srcDataModel.mapReduce(command, function (err, dbres, stats) {
                if (err) {
                    logger.debug(err);
                    responseMessage.retCode = 200000;
                    responseMessage.message = err.toString();
                }
                else {
                    _.forEach(dbres, function (result) {
                        result.value.timestamp = command.query.timestamp.$gte;
                        var entity = new disDataModel(result.value);
                        entity.save(function (error) {
                            if (error) {
                                logger.debug(error);
                            }
                        });
                    });
                    responseMessage.data = stats;
                }
                peerCallback(responseMessage);
            });
        }
    });
};


module.exports = {
    Service: dataStatistics,
    OperationSchemas: OPERATION_SCHEMAS
};