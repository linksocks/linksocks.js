import type { Env } from "./types";

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
linksocks provider -u https://${host} -t your_token -c your_connector_token

For connector:
linksocks connector -u https://${host} -t your_connector_token

LinkSocks client can be downloaded at https://github.com/zetxtech/linksocks`;

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

async function handleWebsocket(request: Request, env: Env, tokenHash: string, isProvider: boolean): Promise<Response> {
  // Validate request
  if (request.headers.get("Upgrade") !== "websocket") {
    return new Response("Expected WebSocket", { status: 426 });
  }

  let relayId: DurableObjectId;

  if (!isProvider) {
    const token = env.TOKEN.get(env.TOKEN.idFromName("main"));
    const relayStr = await token.getRelay(tokenHash);
    if (!relayStr) {
      let pair = new WebSocketPair();
      const [client, server] = [pair[0], pair[1]];
      server.accept();
      const response: AuthResponseMessage = {
        success: false,
        error: `invalid token (${request.url})`,
        getType: () => MessageType.AuthResponse,
      };
      server.send(packMessage(response));
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    relayId = env.RELAY.idFromString(relayStr);
  } else {
    // For connector, create or get a relay based on the token
    relayId = env.RELAY.idFromName(tokenHash);
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

  // Forward to relay
  return await relay.fetch(new Request(newUrl, request));
}
