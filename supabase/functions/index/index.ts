// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.

import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
//import { createClient, SupabaseClient } from 'https://esm.sh/@supabase/supabase-js@2'
import { corsHeaders } from '../_shared/cors.ts'
import { InfluxDB, flux } from 'https://unpkg.com/@influxdata/influxdb-client-browser/dist/index.browser.mjs'
import moment from "https://deno.land/x/momentjs@2.29.1-deno/mod.ts";

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

const queryHistoryRows = async (clientQuery: string): Promise<Response> => {
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
  const response = {
    timePeriod: timePeriod,
    readings: readings
  }
  return new Response(JSON.stringify(response),
  {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
    status: 200
  });
}

async function getHistory(params: URLSearchParams): Promise<Response> {
  const range: number = parseInt(params.get('range') || '0');
  const frequency = range * 5;
  const scaleHistoryQuery = `
    from(bucket: "${influxParameters.bucket}")
        |> range(start: -${range}h)
        |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
        |> filter(fn: (r) => r["_field"] == "weight")
        |> filter(fn: (r) => r["user_id"] == "test-user1")
        |> aggregateWindow(every: ${frequency}s, fn: mean)
        |> fill(column: "_value", usePrevious: true)
        |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
        |> yield(name: "mean")`;
  const clientQuery: string = flux`` + scaleHistoryQuery;
  return queryHistoryRows(clientQuery);
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

const queryAccumulatedRows = async (clientQuery: string): Promise<Response> => {
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
  const response = {
    timePeriod: timePeriod,
    readings: readings
  }
  return new Response(JSON.stringify(response), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
    status: 200
  });
}

async function getAccumulated(params: URLSearchParams): Promise<Response> {
  const range: number = parseInt(params.get('range') || '0');
  const frequency = range * 5;
  const scaleHistoryQuery = `
  from(bucket: "${influxParameters.bucket}")
      |> range(start: -${range}h)
      |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
      |> filter(fn: (r) => r["_field"] == "weight")
      |> filter(fn: (r) => r["user_id"] == "test-user1")
      |> aggregateWindow(every: ${frequency}s, fn: mean)
      |> fill(column: "_value", usePrevious: true)
      |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
      |> yield(name: "mean")`;
  const clientQuery: string = flux`` + scaleHistoryQuery;
  return queryAccumulatedRows(clientQuery);
}

const queryLatest = async (clientQuery: string): Promise<Response> => {
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
  return new Response(JSON.stringify(array), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
    status: 200
  });
}

async function getLatest(): Promise<Response> {
  let array = [];
  const scaleCurrentQuery = `
  from(bucket: "${influxParameters.bucket}")
      |> range(start: -2d)
      |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
      |> filter(fn: (r) => r["_field"] == "weight")
      |> group(columns: ["device_id"])
      |> last()
      |> yield(name: "latest")`;
  let clientQuery = flux`` + scaleCurrentQuery;
  return queryLatest(clientQuery);
}

const returnError = (error: any): Response => {
  console.error(error);
  return new Response(JSON.stringify({ error: error.message }), {
    headers: { ...corsHeaders, "Content-Type": "application/json" },
    status: 400
  })
}

serve((req: any): Response | Promise<Response> => {
  const { method } = req;
  const url = new URL(req.url);
  const params: URLSearchParams = url.searchParams;
  if (method === 'OPTIONS') {
    const response = new Response('ok', { headers: corsHeaders })
    return response
  }
  try {
    switch (true) {
      case method === 'GET' && url.pathname === '/index/scales/history':
        return getHistory(params);
      case method === 'GET' && url.pathname === '/index/scales/accumulated':
        return getAccumulated(params);
      case method === 'GET' && url.pathname === '/index/scales/latest':
        return getLatest();
      default:
        return returnError("Invalid url parameter");
    }
  } catch (error) {
    return returnError(error);
  }
})

// To invoke:
// curl -i --location --request GET 'http://localhost:54321/functions/v1/index/scales/history' \
//  --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
//   --header 'Content-Type: application/json' \
//   --data '{"method":"GET"}'

//curl -L -X GET 'http://localhost:54321/functions/v1/index/scales/history?range=1' \
//-H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0'

// To invoke remote deployed
//curl -L -X GET 'https://vfiomlqwajbenjwswajz.functions.supabase.co/index/scales/history' -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZmaW9tbHF3YWpiZW5qd3N3YWp6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzQwMjc1MTYsImV4cCI6MTk4OTYwMzUxNn0.hvG2Wpfq3SHFq1I6SW_YJZ71ge-0y6ksEXuEjbkgnKM' --data '{"name":"Functions"}'