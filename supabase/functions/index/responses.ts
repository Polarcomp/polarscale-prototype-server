import { corsHeaders } from '../_shared/cors.ts'

const responseOK = (response: string): Response => {
    return new Response(response,
    {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
      status: 200
    });
}

const responseError = (response: string): Response => {
    return new Response(response,
    {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
      status: 400
    });
}

export { responseOK, responseError };