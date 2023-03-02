import moment from "https://deno.land/x/momentjs@2.29.1-deno/mod.ts";
import { InfluxDB } from 'https://unpkg.com/@influxdata/influxdb-client-browser/dist/index.browser.mjs'

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
  
const queryRows = async (clientQuery: string): Promise<{ timePeriod: {start: number; stop: number; }; readings: any[];}> => {
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
  const accumulatedRows = await queryRows(clientQuery);
  return { timePeriod: accumulatedRows.timePeriod, total: accumulatedRows.readings.pop()};
}
  
export { queryRows, queryLatest, queryTotal };