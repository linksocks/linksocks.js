// Raw frame builders for the protocol V2 message types, shared by the
// protocol V2 relay tests.

export function channelOpenFrame(version, channelId, resume) {
  const frame = new Uint8Array(resume ? 2 + 16 + 1 : 2 + 16);
  frame.set([version, 0x1a]);
  frame.set(channelId, 2);
  if (resume) frame[18] = 1;
  return frame;
}

export function sealedFrame(version, channelId, data) {
  const frame = new Uint8Array(2 + 16 + 4 + data.length);
  frame.set([version, 0x1b]);
  frame.set(channelId, 2);
  new DataView(frame.buffer).setUint32(18, data.length);
  frame.set(data, 22);
  return frame;
}

// directSealedFrame builds a DirectSealedMessage (0x1c) carrying an opaque
// signaling envelope, mirroring what a Go peer sends for encrypted direct-mode
// negotiation.
export function directSealedFrame(version, body) {
  const json = new TextEncoder().encode(JSON.stringify(body));
  const frame = new Uint8Array(2 + 4 + json.length);
  frame.set([version, 0x1c]);
  new DataView(frame.buffer).setUint32(2, json.length);
  frame.set(json, 6);
  return frame;
}
