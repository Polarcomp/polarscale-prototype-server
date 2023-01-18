#!/usr/bin/env node
const express = require('express');
const cors = require('cors');
const { InfluxDB, flux } = require('@influxdata/influxdb-client');
const moment = require('moment');
const dotenv = require('dotenv');
dotenv.config();

const baseURL = process.env.INFLUX_URL || "https://europe-west1-1.gcp.cloud2.influxdata.com";
const influxToken = process.env.INFLUX_TOKEN;
const orgID = process.env.ORG_ID;
const bucket = process.env.INFLUX_BUCKET;

// connect to influxdb
const influxDB = new InfluxDB({ url: baseURL, token: influxToken });
const queryApi = influxDB.getQueryApi(orgID);

const app = express();
app.use(cors());
const port = 8081;

const extractReadings = (obj) => {
    const res = {}
    const scales = Object.keys(obj).filter(key => key.startsWith('id_'));
    scales.forEach(id => res[id] = obj[id] ? obj[id] : 0);
    return res;
}

app.get('/scales/history', (req,res) => {
    let readings = [];
    let timePeriod = {};
    const frequency = (req.query.range * 5);
    const scaleHistoryQuery = `
    from(bucket: "${bucket}")
        |> range(start: -${req.query.range}h)
        |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
        |> filter(fn: (r) => r["_field"] == "weight")
        |> filter(fn: (r) => r["user_id"] == "test-user1")
        |> aggregateWindow(every: ${frequency}s, fn: mean)
        |> fill(column: "_value", usePrevious: true)
        |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
        |> yield(name: "mean")`;
    let clientQuery = flux`` + scaleHistoryQuery
    queryApi.queryRows(clientQuery, {
        next(row, tableMeta) {
            let obj = tableMeta.toObject(row);
            let point = {...extractReadings(obj)}
            point["timestamp"] = parseInt(moment(obj._time).format('X'));
            readings.push(point);
            if (!timePeriod.start || !timePeriod.stop)
                timePeriod = {
                    start: parseInt(moment(obj._start).format('X')),
                    stop: parseInt(moment(obj._stop).format('X'))
                }

        },
        error(error) {
            console.error("Error: "+ error);
            res.end();
        },
        complete() {
            console.log('\nFinished SUCCESS');
            const response = {
                timePeriod: timePeriod,
                readings: readings
            }
            res.end(JSON.stringify(response));
        }
    })
})

const accumulateValues = (obj, previousValues, totals) => {
    const currentValues = {};
    const diffs = {};
    const scales = Object.keys(obj).filter(key => key.startsWith('id_'));
    scales.forEach(id => {
        currentValues[id] = obj[id] ? obj[id] : 0;
        diffs[id] = currentValues[id] - previousValues[id];
        if (!(id in totals))
            totals[id] = 0;
        totals[id] += diffs[id] > 0 ? diffs[id] : 0;
        previousValues[id] = currentValues[id];
    });
    return totals;
}

app.get('/scales/accumulated', (req, res) => {
    let previousValues = {};
    let totals = {};
    let readings = [];
    let timePeriod = {};
    const frequency = (req.query.range * 5);
    const scaleHistoryQuery = `
    from(bucket: "${bucket}")
        |> range(start: -${req.query.range}h)
        |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
        |> filter(fn: (r) => r["_field"] == "weight")
        |> filter(fn: (r) => r["user_id"] == "test-user1")
        |> aggregateWindow(every: ${frequency}s, fn: mean)
        |> fill(column: "_value", usePrevious: true)
        |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
        |> yield(name: "mean")`;
    let clientQuery = flux`` + scaleHistoryQuery
    queryApi.queryRows(clientQuery, {
        next(row, tableMeta) {
            let obj = tableMeta.toObject(row);
            let point = {...accumulateValues(obj, previousValues, totals)};
            point["timestamp"] = parseInt(moment(obj._time).format('X'));
            readings.push(point);
            if (!timePeriod.start || !timePeriod.stop)
                timePeriod = {
                    start: parseInt(moment(obj._start).format('X')),
                    stop: parseInt(moment(obj._stop).format('X'))
                }
        },
        error(error) {
            console.error("Error: "+ error);
            res.end();
        },
        complete() {
            console.log('\nFinished SUCCESS');
            const response = {
                timePeriod: timePeriod,
                readings: readings
            }
            res.end(JSON.stringify(response));
        }
    })
})

app.get('/scales/latest', (req, res) => {
    let array = [];
    const scaleCurrentQuery = `
    from(bucket: "${bucket}")
        |> range(start: -2d)
        |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
        |> filter(fn: (r) => r["_field"] == "weight")
        |> group(columns: ["device_id"])
        |> last()
        |> yield(name: "latest")`;
    let clientQuery = flux`` + scaleCurrentQuery;
    queryApi.queryRows(clientQuery, {
        next(row, tableMeta) {
            let point = {};
            let obj = tableMeta.toObject(row);
            point["time"] = obj._time;
            point["weight"] = obj._value;
            point["device_id"] = obj.device_id;
            array.push(point);
        },
        error(error) {
            console.error("Error: ", error);
            res.end();
        },
        complete() {
            console.log('\nLatest readings success');
            res.end(JSON.stringify(array));
        }
    })
})

app.listen(port, () => {
    console.log(`listening on port: ${port}`);
})