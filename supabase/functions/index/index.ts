// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.

import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient, SupabaseClient } from 'https://esm.sh/@supabase/supabase-js@2'
import { queryHistoryRows, queryAccumulatedRows, queryLatest, queryTotal } from "./influxdbQueries.ts"
import { corsHeaders } from '../_shared/cors.ts'
import { responseOK, responseError } from "./responses.ts"
import { flux } from 'https://unpkg.com/@influxdata/influxdb-client-browser/dist/index.browser.mjs'
import * as uuid from "https://deno.land/std@0.175.0/uuid/mod.ts";

const influxParameters = {
  url: Deno.env.get('INFLUX_URL'),
  token: Deno.env.get('INFLUX_TOKEN'),
  org: Deno.env.get('ORG_ID'),
  bucket: Deno.env.get('INFLUX_BUCKET')
}

async function getScales(supabaseClient: SupabaseClient, params: URLSearchParams): Promise<Response> {
  const user_id: string = params.get('user_id') || '';
  const {data, error} = await supabaseClient.from('scales').select('device_id, name').eq('user_id', user_id);
  if (error) throw error;
  return responseOK(JSON.stringify(data));
}

async function getHistory(params: URLSearchParams): Promise<Response> {
  const range: number = parseInt(params.get('range') || '0');
  const user_id: string = params.get('user_id') || '';
  const frequency = range * 5;
  const scaleHistoryQuery = `
    from(bucket: "${influxParameters.bucket}")
        |> range(start: -${range}h)
        |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
        |> filter(fn: (r) => r["_field"] == "weight")
        |> filter(fn: (r) => r["user_id"] == "${user_id}")
        |> aggregateWindow(every: ${frequency}s, fn: mean)
        |> fill(column: "_value", usePrevious: true)
        |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
        |> yield(name: "mean")`;
  const clientQuery: string = flux`` + scaleHistoryQuery;
  const response = await queryHistoryRows(clientQuery);
  return responseOK(JSON.stringify(response));
}

async function getAccumulated(params: URLSearchParams): Promise<Response> {
  const range: number = parseInt(params.get('range') || '0');
  const user_id: string = params.get('user_id') || '';
  const frequency = range * 5;
  const scaleHistoryQuery = `
  from(bucket: "${influxParameters.bucket}")
      |> range(start: -${range}h)
      |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
      |> filter(fn: (r) => r["_field"] == "weight")
      |> filter(fn: (r) => r["user_id"] == "${user_id}")
      |> aggregateWindow(every: ${frequency}s, fn: mean)
      |> fill(column: "_value", usePrevious: true)
      |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
      |> yield(name: "mean")`;
  const clientQuery: string = flux`` + scaleHistoryQuery;
  const response = await queryAccumulatedRows(clientQuery);
  return responseOK(JSON.stringify(response));
}

async function getLatest(params: URLSearchParams): Promise<Response> {
  const user_id: string = params.get('user_id') || '';
  const scaleCurrentQuery = `
  from(bucket: "${influxParameters.bucket}")
      |> range(start: -2d)
      |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
      |> filter(fn: (r) => r["_field"] == "weight")
      |> filter(fn: (r) => r["user_id"] == "${user_id}")
      |> group(columns: ["device_id"])
      |> last()
      |> yield(name: "latest")`;
  const clientQuery = flux`` + scaleCurrentQuery;
  const response = await queryLatest(clientQuery);
  return responseOK(JSON.stringify(response));
}

async function getTotal(params: URLSearchParams): Promise<Response> {
  const range: number = parseInt(params.get('range') || '0');
  const user_id: string = params.get('user_id') || '';
  const frequency = range * 5;
  const scaleHistoryQuery = `
  from(bucket: "${influxParameters.bucket}")
      |> range(start: -${range}h)
      |> filter(fn: (r) => r["_measurement"] == "weight_measurement")
      |> filter(fn: (r) => r["_field"] == "weight")
      |> filter(fn: (r) => r["user_id"] == "${user_id}")
      |> aggregateWindow(every: ${frequency}s, fn: mean)
      |> fill(column: "_value", usePrevious: true)
      |> pivot(rowKey:["_time"], columnKey: ["device_id"], valueColumn: "_value")
      |> yield(name: "mean")`;
  const clientQuery: string = flux`` + scaleHistoryQuery;
  const response = await queryTotal(clientQuery);
  return responseOK(JSON.stringify(response));
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
    const supabaseClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_ANON_KEY') ?? '',
      { global: { headers: { Authorization: req.headers.get('Authorization') ! } } }
    )
    if (!uuid.validate(params.get('user_id') || '')) {
      return responseError(JSON.stringify({ error: 'invalid user id'}))
    }
    switch (true) {
      case method === 'GET' && url.pathname === '/index/scales/scales':
        return getScales(supabaseClient, params);
      case method === 'GET' && url.pathname === '/index/scales/history':
        return getHistory(params);
      case method === 'GET' && url.pathname === '/index/scales/accumulated':
        return getAccumulated(params);
      case method === 'GET' && url.pathname === '/index/scales/latest':
        return getLatest(params);
      case method === 'GET' && url.pathname === '/index/scales/total':
        return getTotal(params);
      default:
        return responseError(JSON.stringify({ error: "Invalid URL parameter"}));
    }
  } catch (error) {
    return responseError(JSON.stringify(error));
  }
})

// To invoke:
// curl -i --location --request GET 'http://localhost:54321/functions/v1/index/scales/history' \
//  --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0' \
//   --header 'Content-Type: application/json' \
//   --data '{"method":"GET"}'

//curl -L -X GET 'http://localhost:54321/functions/v1/index/scales/history?range=1&user_id=e32f5583-c101-4bac-97eb-b77fe01109f1' \
//-H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0'

//curl -L -X GET 'http://localhost:54321/functions/v1/index/scales/scales?user_id=e32f5583-c101-4bac-97eb-b77fe01109f1' \
//-H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0'

// To invoke remote deployed
//curl -L -X GET 'https://vfiomlqwajbenjwswajz.functions.supabase.co/index/scales/history?range=1&user_id=e32f5583-c101-4bac-97eb-b77fe01109f1' \
//-H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZmaW9tbHF3YWpiZW5qd3N3YWp6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzQwMjc1MTYsImV4cCI6MTk4OTYwMzUxNn0.hvG2Wpfq3SHFq1I6SW_YJZ71ge-0y6ksEXuEjbkgnKM' \
//--data '{"name":"Functions"}'

//curl -L -X GET 'https://vfiomlqwajbenjwswajz.functions.supabase.co/index/scales/scales?user_id=e32f5583-c101-4bac-97eb-b77fe01109f1' \
//-H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZmaW9tbHF3YWpiZW5qd3N3YWp6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2NzQwMjc1MTYsImV4cCI6MTk4OTYwMzUxNn0.hvG2Wpfq3SHFq1I6SW_YJZ71ge-0y6ksEXuEjbkgnKM' \
//--data '{"name":"Functions"}'