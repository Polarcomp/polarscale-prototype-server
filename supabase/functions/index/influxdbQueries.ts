import { corsHeaders } from '../_shared/cors.ts'
import moment from "https://deno.land/x/momentjs@2.29.1-deno/mod.ts";
import { InfluxDB } from 'https://unpkg.com/@influxdata/influxdb-client-browser/dist/index.browser.mjs'
import { responseOK, responseError } from './responses.ts';

const influxParameters = {
  url: Deno.env.get('INFLUX_URL'),
  token: Deno.env.get('INFLUX_TOKEN'),
  org: Deno.env.get('ORG_ID'),
  bucket: Deno.env.get('INFLUX_BUCKET')
}
const queryApi = new InfluxDB({ url: influxParameters.url, token: influxParameters.token }).getQueryApi(influxParameters.org);

const extractReadings = (obj: any) => {
    const res: {[index: string]: any } = {};
    const scales: string[] = Object.keys(obj).filter(key => key.startsWith('id_'));
    scales.forEach(id => res[id] = obj[id] ? obj[id] : 0);
    return res;
  }
  
const queryHistoryRows = async (clientQuery: string): Promise<{ timePeriod: {start: number; stop: number; }; readings: any[];}> => {
    const readings: any[] = [];
    let timePeriod = {
      start: 0,
      stop: 0
    }
    for await (const {values, tableMeta} of queryApi.iterateRows(clientQuery)) {
      const obj = tableMeta.toObject(values);
      let point: { timestamp: number; } = { timestamp: 0, ...extractReadings(obj)};
      point["timestamp"] = parseInt(moment(obj._time).format('X'));
      readings.push(point);
      if (!timePeriod.start || !timePeriod.stop)
      {
        timePeriod = {
          start: parseInt(moment(obj._start).format('X')),
          stop: parseInt(moment(obj._stop).format('X'))
        }
      }
    }
    return { timePeriod: timePeriod, readings: readings }
}

const accumulateValues = (obj: any, previousValues: any, totals: any) => {
    const currentValues: {[index: string]: any } = {};
    const diffs: {[index: string]: any } = {};
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
  
const queryAccumulatedRows = async (clientQuery: string): Promise<{ timePeriod: {start: number; stop: number; }; readings: any[];}> => {
    let readings: any[] = [];
    let previousValues = {};
    let totals = {};
    let timePeriod = {
      start: 0,
      stop: 0
    }
    for await (const {values, tableMeta} of queryApi.iterateRows(clientQuery)) {
      const obj = tableMeta.toObject(values);
      let point: any = {...accumulateValues(obj, previousValues, totals)}
      point["timestamp"] = parseInt(moment(obj._time).format('X'));
      readings.push(point);
      if (!timePeriod.start || !timePeriod.stop)
      {
        timePeriod = {
          start: parseInt(moment(obj._start).format('X')),
          stop: parseInt(moment(obj._stop).format('X'))
        }
      }
    }
    return { timePeriod: timePeriod, readings: readings }
}

const queryLatest = async (clientQuery: string): Promise<any[]> => {
    let array = [];
    for await (const {values, tableMeta} of queryApi.iterateRows(clientQuery)) {
      let obj = tableMeta.toObject(values);
      const point = {
        time: obj._time,
        weight: obj._value,
        device_id: obj.device_id
      }
      array.push(point);
    }
    return array;
}

const queryTotal = async (clientQuery: string): Promise<{ timePeriod: {start: number; stop: number;}; total: any[]}> => {
  const accumulatedRows = await queryAccumulatedRows(clientQuery);
  return { timePeriod: accumulatedRows.timePeriod, total: accumulatedRows.readings.pop()};
}
  
export { queryHistoryRows, queryAccumulatedRows, queryLatest, queryTotal };