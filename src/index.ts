import type { Env } from "./types";
import zxcvbn from "zxcvbn";

import { handleErrors } from "./common";
import { AuthResponseMessage, packMessage, MessageType } from "./message";
export { Relay } from "./relay";
export { Token } from "./token";

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    return await handleErrors(request, async () => {
      const url = new URL(request.url);
      const path = url.pathname.split("/");
      
      if (path[1] === "") {
        const host = url.host;
        const welcomeMessage = `LinkSocks.js is running. You can use LinkSocks client to connect to it:

For network provider:
linksocks provider -u https://${host} -c your_connector_token

For connector:
linksocks connector -u https://${host} -t your_connector_token

LinkSocks client can be downloaded at https://github.com/linksocks/linksocks`;

        return new Response(welcomeMessage, {
          status: 200,
          headers: { "Content-Type": "text/plain" }
        });
      }
      
      if (path[1] === "socket" && request.headers.get("Upgrade") === "websocket") {
        const token = url.searchParams.get("token");
        const reverse = url.searchParams.get("reverse");
        
        if (!token) {
          throw Error('Missing token parameter.');
        }
        
        return await handleWebsocket(request, env, token, reverse === "1" || reverse === "true");
      }

      return new Response("Not found", { status: 404 });
    });
  },
};

function isTokenComplexEnough(token: string): { valid: boolean; reason?: string } {
  if (token.length < 8) {
    return { valid: false, reason: "Token must be at least 8 characters" };
  }
  
  const result = zxcvbn(token);
  // score: 0 = too guessable, 1 = very guessable, 2 = somewhat guessable, 3 = safely unguessable, 4 = very unguessable
  // Require at least score 2 (somewhat guessable)
  if (result.score < 2) {
    const feedback = result.feedback.warning || result.feedback.suggestions[0] || "Token is too weak";
    return { valid: false, reason: feedback };
  }
  
  return { valid: true };
}

function rejectWithMessage(message: string): Response {
  const pair = new WebSocketPair();
  const [client, server] = [pair[0], pair[1]];
  server.accept();
  const response: AuthResponseMessage = {
    success: false,
    error: message,
    getType: () => MessageType.AuthResponse,
  };
  server.send(packMessage(response));
  return new Response(null, { status: 101, webSocket: client });
}

async function handleWebsocket(request: Request, env: Env, token: string, isProvider: boolean): Promise<Response> {
  // Validate request
  if (request.headers.get("Upgrade") !== "websocket") {
    return new Response("Expected WebSocket", { status: 426 });
  }

  let relayId: DurableObjectId;
  let actualToken = token;

  if (!isProvider) {
    const tokenDO = env.TOKEN.get(env.TOKEN.idFromName("main"));
    const relayStr = await tokenDO.getRelay(token);
    if (!relayStr) {
      return rejectWithMessage(`invalid token (${request.url})`);
    }
    relayId = env.RELAY.idFromString(relayStr);
  } else {
    // For provider: validate token complexity (except "anonymous")
    if (token !== "anonymous") {
      const validation = isTokenComplexEnough(token);
      if (!validation.valid) {
        return rejectWithMessage(validation.reason!);
      }
    }
    
    if (token === "anonymous") {
      actualToken = crypto.randomUUID();
    }
    relayId = env.RELAY.idFromName(actualToken);
  }

  // Check if the request is from APAC region and set locationHint accordingly
  const apacCountries = ["CN", "HK", "JP", "SG", "MO", "TW", "KR"];
  const isFromApac = request.cf && request.cf.country && apacCountries.includes(request.cf.country as string);
  const relay = isFromApac 
    ? env.RELAY.get(relayId, { locationHint: "apac" })
    : env.RELAY.get(relayId);

  // Add provider/connector information to the URL for the relay
  const newUrl = new URL(request.url);
  newUrl.pathname = isProvider ? "/provider" : "/connector";
  newUrl.searchParams.set("actualToken", actualToken);

  // Forward to relay
  return await relay.fetch(new Request(newUrl, request));
}
