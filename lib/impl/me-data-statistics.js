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
var SUPPORTED_DEVICE_TYPE = [
  "010001000004",
  "01000C000004",
  "01000D000004",
  "04110E0E0001",
  "04110F0F0001",
  "010100000000",
  "060A08000000"
];
var REPORT_DATA_TYPE = ["dailyReport", "monthlyReport", "annualReport", "allReport"];
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
const INVERTER_TYPES = [
  "010001000004", "01000C000004",
  "01000D000004"
];
const DEVICE_AGGREGATE_METHOD = {
  "050608070001": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumTemp = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      if (null !== item.data.temperature && undefined !== item.data.temperature) {
        sumTemp += item.data.temperature
      }
    });
    record.data.temperature = sumTemp / len;
    return record;
  },
  "040B08040004": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumPower = 0;
    var sumEnergyUsed = 0;
    var sumEnergySaved = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      if (null !== item.data.power && undefined !== item.data.power) {
        sumPower += item.data.power;
      }
      if (null !== item.data.energyUsed && undefined !== item.data.energyUsed) {
        sumEnergyUsed += item.data.energyUsed;
      }
      if (null !== item.data.energySaved && undefined !== item.data.energySaved) {
        sumEnergySaved += item.data.energySaved;
      }
    });
    record.data.power = sumPower / len;
    record.data.energyUsed = sumEnergyUsed;
    record.data.energySaved = sumEnergySaved;
    return record;
  },
  "040B01000001": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumToUserPower = 0;
    var sumToGridPower = 0;
    var sumToGrid = 0;
    var sumToUser = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      if (null !== item.data.toGrid && undefined !== item.data.toGrid) {
        sumToGrid += item.data.toGrid;
      }
      if (null !== item.data.toUser && undefined !== item.data.toUser) {
        sumToUser += item.data.toUser;
      }
      if (null !== item.data.toUserPower && undefined !== item.data.toUserPower) {
        sumToUserPower += item.data.toUserPower;
      }
      if (null !== item.data.toGridPower && undefined !== item.data.toGridPower) {
        sumToGridPower += item.data.toGridPower;
      }
    });
    record.data.toUserPower = sumToUserPower / len;
    record.data.toGridPower = sumToGridPower / len;
    record.data.toGrid = sumToGrid;
    record.data.toUser = sumToUser;
    return record;
  },
  "040B01000005": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumPacToGrid = 0;
    var sumPacToUser = 0;
    var sumEDisChargeTotal = 0;
    var sumEChargeTotal = 0;
    var sumEToGridTotal = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      if (null !== item.data.pacToGrid && undefined !== item.data.pacToGrid) {
        sumPacToGrid += item.data.pacToGrid;
      }
      if (null !== item.data.pacToUser && undefined !== item.data.pacToUser) {
        sumPacToUser += item.data.pacToUser;
      }
      if (null !== item.data.eDisChargeTotal && undefined !== item.data.eDisChargeTotal) {
        sumEDisChargeTotal += item.data.eDisChargeTotal;
      }
      if (null !== item.data.eChargeTotal && undefined !== item.data.eChargeTotal) {
        sumEChargeTotal += item.data.eChargeTotal;
      }
      if (null !== item.data.eToGridTotal && undefined !== item.data.eToGridTotal) {
        sumEToGridTotal += item.data.eToGridTotal;
      }
    });
    record.data.pacToGrid = sumPacToGrid / len;
    record.data.pacToUser = sumPacToUser / len;
    record.data.eDisChargeTotal = sumEDisChargeTotal;
    record.data.eChargeTotal = sumEChargeTotal;
    record.data.eToGridTotal = sumEToGridTotal;
    return record;
  },
  "040B01000004": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumCurrentPower = 0;
    var sumTotalEnergy = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      if (null !== item.data.currentPower && undefined !== item.data.currentPower) {
        sumCurrentPower += item.data.currentPower;
      }
      if (null !== item.data.totalEnergy && undefined !== item.data.totalEnergy) {
        sumTotalEnergy += item.data.totalEnergy;
      }
    });
    record.data.currentPower = sumCurrentPower / len;
    record.data.totalEnergy = sumTotalEnergy;
    return record;
  },
  "010100000000": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumInverterEnergyTotal = 0;
    var sumPPvTotal = 0;
    var sumPAcTotal = 0;
    var sumEnergyPositiveActive = 0;
    var sumEnergyReverseActive = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      sumInverterEnergyTotal += item.data.inverterEnergyTotal ? item.data.inverterEnergyTotal : 0;
      sumPPvTotal += item.data.PPvTotal ? item.data.PPvTotal : 0;
      sumPAcTotal += item.data.PAcTotal ? item.data.PAcTotal : 0;
      sumEnergyPositiveActive += item.data.energyPositiveActive ? item.data.energyPositiveActive : 0;
      sumEnergyReverseActive += item.data.energyReverseActive ? item.data.energyReverseActive : 0;
    });
    record.data.inverterEnergyTotal = sumInverterEnergyTotal;
    record.data.PPvTotal = sumPPvTotal / len;
    record.data.PAcTotal = sumPAcTotal / len;
    record.data.energyPositiveActive = sumEnergyPositiveActive;
    record.data.energyReverseActive = sumEnergyReverseActive;
    return record;
  },
  "060A08000000": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumInverterEnergyTotal = 0;
    var sumPPvTotal = 0;
    var sumPAcTotal = 0;
    var sumEnergyPositiveActive = 0;
    var sumEnergyReverseActive = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      sumInverterEnergyTotal += item.data.inverterEnergyTotal ? item.data.inverterEnergyTotal : 0;
      sumPPvTotal += item.data.PPvTotal ? item.data.PPvTotal : 0;
      sumPAcTotal += item.data.PAcTotal ? item.data.PAcTotal : 0;
      sumEnergyPositiveActive += item.data.energyPositiveActive ? item.data.energyPositiveActive : 0;
      sumEnergyReverseActive += item.data.energyReverseActive ? item.data.energyReverseActive : 0;
    });
    record.data.inverterEnergyTotal = sumInverterEnergyTotal;
    record.data.PPvTotal = sumPPvTotal / len;
    record.data.PAcTotal = sumPAcTotal / len;
    record.data.energyPositiveActive = sumEnergyPositiveActive;
    record.data.energyReverseActive = sumEnergyReverseActive;
    return record;
  },
  "010001000004": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumEnergyTotal = 0;
    var sumPac = 0;
    var sumRac = 0;
    var sumPpv = 0;
    var sumPv1Cur = 0;
    var sumPv2Cur = 0;
    var sumPv3Cur = 0;
    var sumPv4Cur = 0;
    var sumVpv1 = 0;
    var sumVpv2 = 0;
    var sumVpv3 = 0;
    var sumVpv4 = 0;
    var sumVac1 = 0;
    var sumVac2 = 0;
    var sumVac3 = 0;
    var sumIac1 = 0;
    var sumIac2 = 0;
    var sumIac3 = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      sumEnergyTotal += item.data.energyTotal ? item.data.energyTotal : 0;
      sumPac += item.data.pac ? item.data.pac : 0;
      sumRac += item.data.rac ? item.data.rac : 0;
      sumPpv += item.data.ppv ? item.data.ppv : 0;

      sumPv1Cur += item.data.pv1Cur ? item.data.pv1Cur : 0;
      sumPv2Cur += item.data.pv2Cur ? item.data.pv2Cur : 0;
      sumPv3Cur += item.data.pv3Cur ? item.data.pv3Cur : 0;
      sumPv4Cur += item.data.pv4Cur ? item.data.pv4Cur : 0;

      sumVpv1 += item.data.vpv1 ? item.data.vpv1 : 0;
      sumVpv2 += item.data.vpv2 ? item.data.vpv2 : 0;
      sumVpv3 += item.data.vpv3 ? item.data.vpv3 : 0;
      sumVpv4 += item.data.vpv4 ? item.data.vpv4 : 0;

      sumVac1 += item.data.vac1 ? item.data.vac1 : 0;
      sumVac2 += item.data.vac2 ? item.data.vac2 : 0;
      sumVac3 += item.data.vac3 ? item.data.vac3 : 0;
      sumIac1 += item.data.iac1 ? item.data.iac1 : 0;
      sumIac2 += item.data.iac2 ? item.data.iac2 : 0;
      sumIac3 += item.data.iac3 ? item.data.iac3 : 0;
    });
    record.data.energyTotal = sumEnergyTotal;
    record.data.pac = sumPac / len;
    record.data.rac = sumRac / len;
    record.data.ppv = sumPpv / len;

    record.data.pv1Cur = sumPv1Cur / len;
    record.data.pv2Cur = sumPv2Cur / len;
    record.data.pv3Cur = sumPv3Cur / len;
    record.data.pv4Cur = sumPv4Cur / len;

    record.data.vpv1 = sumVpv1 / len;
    record.data.vpv2 = sumVpv2 / len;
    record.data.vpv3 = sumVpv3 / len;
    record.data.vpv4 = sumVpv4 / len;

    record.data.vac1 = sumVac1 / len;
    record.data.vac2 = sumVac2 / len;
    record.data.vac3 = sumVac3 / len;

    record.data.iac1 = sumIac1 / len;
    record.data.iac2 = sumIac2 / len;
    record.data.iac3 = sumIac3 / len;

    return record;
  },
  "01000C000004": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumEnergyTotal = 0;
    var sumPac = 0;
    var sumRac = 0;
    var sumPpv = 0;
    var sumPv1Cur = 0;
    var sumPv2Cur = 0;
    var sumPv3Cur = 0;
    var sumPv4Cur = 0;
    var sumVpv1 = 0;
    var sumVpv2 = 0;
    var sumVpv3 = 0;
    var sumVpv4 = 0;
    var sumVac1 = 0;
    var sumVac2 = 0;
    var sumVac3 = 0;
    var sumIac1 = 0;
    var sumIac2 = 0;
    var sumIac3 = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      sumEnergyTotal += item.data.energyTotal ? item.data.energyTotal : 0;
      sumPac += item.data.pac ? item.data.pac : 0;
      sumRac += item.data.rac ? item.data.rac : 0;
      sumPpv += item.data.ppv ? item.data.ppv : 0;

      sumPv1Cur += item.data.pv1Cur ? item.data.pv1Cur : 0;
      sumPv2Cur += item.data.pv2Cur ? item.data.pv2Cur : 0;
      sumPv3Cur += item.data.pv3Cur ? item.data.pv3Cur : 0;
      sumPv4Cur += item.data.pv4Cur ? item.data.pv4Cur : 0;

      sumVpv1 += item.data.vpv1 ? item.data.vpv1 : 0;
      sumVpv2 += item.data.vpv2 ? item.data.vpv2 : 0;
      sumVpv3 += item.data.vpv3 ? item.data.vpv3 : 0;
      sumVpv4 += item.data.vpv4 ? item.data.vpv4 : 0;

      sumVac1 += item.data.vac1 ? item.data.vac1 : 0;
      sumVac2 += item.data.vac2 ? item.data.vac2 : 0;
      sumVac3 += item.data.vac3 ? item.data.vac3 : 0;
      sumIac1 += item.data.iac1 ? item.data.iac1 : 0;
      sumIac2 += item.data.iac2 ? item.data.iac2 : 0;
      sumIac3 += item.data.iac3 ? item.data.iac3 : 0;
    });
    record.data.energyTotal = sumEnergyTotal;
    record.data.pac = sumPac / len;
    record.data.rac = sumRac / len;
    record.data.ppv = sumPpv / len;

    record.data.pv1Cur = sumPv1Cur / len;
    record.data.pv2Cur = sumPv2Cur / len;
    record.data.pv3Cur = sumPv3Cur / len;
    record.data.pv4Cur = sumPv4Cur / len;

    record.data.vpv1 = sumVpv1 / len;
    record.data.vpv2 = sumVpv2 / len;
    record.data.vpv3 = sumVpv3 / len;
    record.data.vpv4 = sumVpv4 / len;

    record.data.vac1 = sumVac1 / len;
    record.data.vac2 = sumVac2 / len;
    record.data.vac3 = sumVac3 / len;

    record.data.iac1 = sumIac1 / len;
    record.data.iac2 = sumIac2 / len;
    record.data.iac3 = sumIac3 / len;

    return record;
  },
  "01000D000004": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumEnergyTotal = 0;
    var sumPac = 0;
    var sumRac = 0;
    var sumPpv = 0;
    var sumPv1Cur = 0;
    var sumPv2Cur = 0;
    var sumPv3Cur = 0;
    var sumPv4Cur = 0;
    var sumVpv1 = 0;
    var sumVpv2 = 0;
    var sumVpv3 = 0;
    var sumVpv4 = 0;
    var sumVac1 = 0;
    var sumVac2 = 0;
    var sumVac3 = 0;
    var sumIac1 = 0;
    var sumIac2 = 0;
    var sumIac3 = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      sumEnergyTotal += item.data.energyTotal ? item.data.energyTotal : 0;
      sumPac += item.data.pac ? item.data.pac : 0;
      sumRac += item.data.rac ? item.data.rac : 0;
      sumPpv += item.data.ppv ? item.data.ppv : 0;

      sumPv1Cur += item.data.pv1Cur ? item.data.pv1Cur : 0;
      sumPv2Cur += item.data.pv2Cur ? item.data.pv2Cur : 0;
      sumPv3Cur += item.data.pv3Cur ? item.data.pv3Cur : 0;
      sumPv4Cur += item.data.pv4Cur ? item.data.pv4Cur : 0;

      sumVpv1 += item.data.vpv1 ? item.data.vpv1 : 0;
      sumVpv2 += item.data.vpv2 ? item.data.vpv2 : 0;
      sumVpv3 += item.data.vpv3 ? item.data.vpv3 : 0;
      sumVpv4 += item.data.vpv4 ? item.data.vpv4 : 0;

      sumVac1 += item.data.vac1 ? item.data.vac1 : 0;
      sumVac2 += item.data.vac2 ? item.data.vac2 : 0;
      sumVac3 += item.data.vac3 ? item.data.vac3 : 0;
      sumIac1 += item.data.iac1 ? item.data.iac1 : 0;
      sumIac2 += item.data.iac2 ? item.data.iac2 : 0;
      sumIac3 += item.data.iac3 ? item.data.iac3 : 0;
    });

    record.data.energyTotal = sumEnergyTotal;
    record.data.pac = sumPac / len;
    record.data.rac = sumRac / len;
    record.data.ppv = sumPpv / len;

    record.data.pv1Cur = sumPv1Cur / len;
    record.data.pv2Cur = sumPv2Cur / len;
    record.data.pv3Cur = sumPv3Cur / len;
    record.data.pv4Cur = sumPv4Cur / len;

    record.data.vpv1 = sumVpv1 / len;
    record.data.vpv2 = sumVpv2 / len;
    record.data.vpv3 = sumVpv3 / len;
    record.data.vpv4 = sumVpv4 / len;

    record.data.vac1 = sumVac1 / len;
    record.data.vac2 = sumVac2 / len;
    record.data.vac3 = sumVac3 / len;

    record.data.iac1 = sumIac1 / len;
    record.data.iac2 = sumIac2 / len;
    record.data.iac3 = sumIac3 / len;

    return record;
  },
  "04110E0E0001": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var sumECombinationActive = 0;
    var sumEPositiveActive = 0;
    var sumEReverseActive = 0;
    var sumEPositiveApparent = 0;
    var sumEReverseApparent = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      sumECombinationActive += item.data.eCombinationActive ? item.data.eCombinationActive : 0;
      sumEPositiveActive += item.data.ePositiveActive ? item.data.ePositiveActive : 0;
      sumEReverseActive += item.data.eReverseActive ? item.data.eReverseActive : 0;
      sumEPositiveApparent += item.data.ePositiveApparent ? item.data.ePositiveApparent : 0;
      sumEReverseApparent += item.data.eReverseApparent ? item.data.eReverseApparent : 0;
    });
    record.data.eCombinationActive = sumECombinationActive;
    record.data.ePositiveActive = sumEPositiveActive;
    record.data.eReverseActive = sumEReverseActive;
    record.data.ePositiveApparent = sumEPositiveApparent;
    record.data.eReverseApparent = sumEReverseApparent;
    return record;
  },
  "04110F0F0001": function (datas) {
    var record = _.cloneDeep(datas[0]);
    if(!record.data){
      record.data = {};
    }
    var len = datas.length;
    var sumWindSpeed = 0;
    var sumRainfall = 0;
    var sumIrradiance = 0;
    var sumSoilTemperature = 0;
    var sumSoilHumidity = 0;
    var sumAirTemperature = 0;
    var sumAirPressure = 0;
    var sumAirHumidity = 0;
    var sumIlluminance = 0;
    var sumEvaporation = 0;
    datas.forEach(function (item) {
      if (!item.data) {
        return;
      }
      sumWindSpeed += item.data.windSpeed ? item.data.windSpeed : 0;
      sumRainfall += item.data.rainfall ? item.data.rainfall : 0;
      sumIrradiance += item.data.irradiance ? item.data.irradiance : 0;
      sumSoilTemperature += item.data.soilTemperature ? item.data.soilTemperature : 0;
      sumSoilHumidity += item.data.soilHumidity ? item.data.soilHumidity : 0;

      sumAirTemperature += item.data.airTemperature ? item.data.airTemperature : 0;
      sumAirPressure += item.data.airPressure ? item.data.airPressure : 0;
      sumAirHumidity += item.data.airHumidity ? item.data.airHumidity : 0;
      sumIlluminance += item.data.illuminance ? item.data.illuminance : 0;
      sumEvaporation += item.data.evaporation ? item.data.evaporation : 0;
    });
    record.data.windSpeed = sumWindSpeed / len;
    record.data.rainfall = sumRainfall / len;
    record.data.irradiance = sumIrradiance / len;
    record.data.soilTemperature = sumSoilTemperature / len;
    record.data.soilHumidity = sumSoilHumidity / len;

    record.data.airTemperature = sumAirTemperature / len;
    record.data.airPressure = sumAirPressure / len;
    record.data.airHumidity = sumAirHumidity / len;
    record.data.illuminance = sumIlluminance / len;
    record.data.evaporation = sumEvaporation / len;
    return record;
  }

};

var isInverter = function (deviceType) {
  var found = _.findIndex(INVERTER_TYPES, function (item) {
    return item === deviceType;
  });
  return -1 !== found;
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
  this.aggregateReportData = function (device, condition, callback) {
    var self = this;
    condition["uuid"] = device.uuid;
    self.getReportData(condition, function (error, datas) {
      if (error) {
        callback(error);
        return;
      }
      var aggregateMethod = DEVICE_AGGREGATE_METHOD[device.type.id];
      var result = null;
      if (datas && datas.length > 0) {
        result = aggregateMethod(datas);
        result.timestamp = condition.timestamp["$gte"];
        delete result["_id"];
        delete result["__v"];
      }
      callback(null, result);
    })
  };
  this.getReportData = function (message, callback) {
    var self = this;
    self.message({
      devices: self.configurator.getConfRandom("services.data_manager"),
      payload: {
        cmdName: "getOrgData",
        cmdCode: "0003",
        parameters: message
      }
    }, function (response) {
      if (200 !== response.retCode) {
        callback({errorId: response.retCode, errorMsg: response.description});
        return;
      }
      callback(null, response.data);
    })
  };
  this.getDevices = function (condition, callback) {
    var self = this;
    var msg = {
      devices: self.configurator.getConfRandom("services.device_manager"),
      payload: {
        cmdName: "getDevice",
        cmdCode: "0003",
        parameters: condition
      }
    };
    self.message(msg, function (response) {
      if (response.retCode === 200) {
        var devices = response.data;
        callback(null, devices);
      }
      else if (response.retCode === 200003) {
        callback({errorId: response.retCode, errorMsg: response.description});
      }
      else {
        callback(null, null);
      }
    });
  };
  this.updateDevice = function (update, callback) {
    var self = this;
    var updateMsg = {
      devices: self.configurator.getConfRandom("services.device_manager"),
      payload: {
        cmdName: "deviceUpdate",
        cmdCode: "0004",
        parameters: update
      }
    };
    if (!util.isFunction(callback)) {
      self.message(updateMsg);
      return;
    }
    self.message(updateMsg, function (response) {
      if (response.retCode === 200) {
        var device = response.data;
        callback(null, device);
      }
      else {
        callback({errorId: response.retCode, errorMsg: response.description});
      }
    });
  };
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
      else if ("annualReport" === dataType) {
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
      logger.debug("**************************************************");
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
    //self.statisticsAll();
    self.scheduleJob = schedule.scheduleJob("3 * * * *", function () {
      self.statisticsAll();
    });
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
  logger.info(message);
  var responseMessage = {retCode: 200, description: "Success.", data: {}};
  self.messageValidate(message, OPERATION_SCHEMAS.statistics, function (error) {
    if (error) {
      responseMessage = error;
      peerCallback(error);
      return;
    }
    async.mapSeries(SUPPORTED_DEVICE_TYPE, function (deviceType, cb) {
      async.waterfall([
        function (innerCallback) {
          self.getDevices({"type.id": deviceType}, function (error, devices) {
            innerCallback(error, devices);
          })
        },
        function (devices, innerCallback) {
          var index = 0;
          var aggregateResult = [];
          var intervalId = setInterval(function (self, msg) {
            if (index >= devices.length) {
              clearInterval(intervalId);
              innerCallback(null, {"type": deviceType, count: index});
              return;
            }
            var dataModel = null;
            var msg1 = _.cloneDeep(msg);
            switch (msg1.dataType) {
              case "dailyReport": {
                msg1.dataType = "dataReport";
                dataModel = self.dailyDataModel;
              }
                break;
              case "monthlyReport": {
                msg1.dataType = "dailyReport";
                dataModel = self.monthlyDataModel;
              }
                break;
              case "annualReport": {
                msg1.dataType = "monthlyReport";
                dataModel = self.yearlyDataModel;
              }
                break;
              case "allReport": {
                msg1.dataType = "annualReport";
                dataModel = self.allDataModel;
              }
            }
            self.aggregateReportData(devices[index++], msg1, function (error, result) {
              error ? aggregateResult.push(null) : aggregateResult.push(result);
              if (!error && result) {
                var query = {
                  "uuid": result.uuid,
                  "timestamp": result.timestamp
                };
                var update = result;
                var updateMsg = null;
                if ("annualReport" === msg.dataType) {
                  if (isInverter(update.type)) {
                    updateMsg = {
                      "uuid": update.uuid,
                      "extra.items.energyMonth": update.data.energyTotal > 0 ? update.data.energyTotal : 0
                    };
                  }
                }
                else if ("allReport" === msg.dataType) {
                  if (isInverter(update.type)) {
                    updateMsg = {
                      "uuid": update.uuid,
                      "extra.items.energyYear": update.data.energyTotal > 0 ? update.data.energyTotal : 0
                    };
                  }
                }
                if (!util.isNullOrUndefined(updateMsg)) {
                  self.updateDevice(updateMsg, function (error, response) {
                    if (error) {
                      logger.info(updateMsg);
                      logger.error(error.errorId, error.errorMsg);
                    }
                  });
                }
                var options = {
                  new: true,
                  upsert: true
                };
                dataModel.findOneAndUpdate(query, update, options, function (error, doc) {
                  if (error) {
                    logger.error(200000, error);
                  }
                  else {
                    //logger.debug(doc)
                  }
                });
              }
            });
          }, 500, self, _.cloneDeep(message));
        }
      ], function (error, result) {
        cb(null, result);
      });
    }, function (error, result) {
      if (error) {
        responseMessage.retCode = error.errorId;
        responseMessage.description = error.errorMsg;
      }
      else {
        responseMessage.data = result;
      }
      peerCallback(responseMessage);
    });
    /*else {


      /!*var srcDataModel = null;
      var disDataModel = null;
      var reduceFun = dataReduce;

      if ("dailyReport" === message.dataType) {
        srcDataModel = self.dataModel;
        disDataModel = self.dailyDataModel;
      }
      else if ("monthlyReport" === message.dataType) {
        srcDataModel = self.dailyDataModel;
        disDataModel = self.monthlyDataModel;
      }
      else if ("annualReport" === message.dataType) {
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
          "uuid":"455e41d4-600b-4679-bc6f-88e598860a76",
          "timestamp": {
            "$gte": new Date(message.timestamp.$gte),
            "$lt": new Date(message.timestamp.$lt)
          }
        },
        map: dataMap,
        reduce: reduceFun,
        verbose: false
      };
      srcDataModel.mapReduce(command, function (err, dbres, stats) {
        if (err) {
          logger.debug(err);
          responseMessage.retCode = 200000;
          responseMessage.message = err.toString();
          peerCallback(responseMessage);
        }
        else {
          logger.debug("======================================================");
          //logger.debug(dbres);
          async.mapSeries(dbres, function (result, callback) {
            if("04110F0F0001" !== result.value.type){
              callback(null, null);
              return;
            }
            delete result.value.__v;
            delete result.value._id;
            result.value.timestamp = command.query.timestamp.$gte;
            var query = {
              "uuid": result.value.uuid,
              "timestamp": result.value.timestamp
            };
            var update = result.value;
            var updateMsg = null;
            if ("annualReport" === message.dataType) {
              if (isInverter(update.type)) {
                updateMsg = {
                  "uuid": update.uuid,
                  "extra.items.energyMonth": update.data.energyTotal > 0 ? update.data.energyTotal : 0
                };
              }
            }
            else if ("allReport" === message.dataType) {
              if (isInverter(update.type)) {
                updateMsg = {
                  "uuid": update.uuid,
                  "extra.items.energyYear": update.data.energyTotal > 0 ? update.data.energyTotal : 0
                };
              }
            }
            if (!util.isNullOrUndefined(updateMsg)) {
              /!*self.updateDevice(updateMsg, function (error, response) {
                if (error) {
                  logger.info(updateMsg);
                  logger.error(error.errorId, error.errorMsg);
                }
              });*!/
            }
            var options = {
              new: true,
              upsert: true
            };
            /!*disDataModel.findOneAndUpdate(query, update, options, function (error, doc) {
              if (error) {
                logger.error(200000, error);
              }
              else {
                //logger.debug(doc)
              }
              callback(null, doc);
            });*!/
            callback(null, null);
          }, function (error, results) {
            responseMessage.data = {statistics: message, stats: stats, results: results};
            peerCallback(responseMessage);
          });
        }
      });*!/
    }*/
  });
};


module.exports = {
  Service: dataStatistics,
  OperationSchemas: OPERATION_SCHEMAS
};