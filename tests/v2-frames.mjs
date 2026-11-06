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
