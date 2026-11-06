// Feature minimums are separate from the current message protocol version.
export const MIN_VERSION_DIRECT = 0x01;
export const MIN_VERSION_MIGRATION = 0x02;
export const MIN_VERSION_WEB = 0x03;
export const MIN_VERSION_WEBSOCKET = 0x04;

export function supportsFeatureVersion(version: number, minimum: number): boolean {
  // Protocol generation 2 is a superset of the generation 0 feature set:
  // peers on it support direct, migration, web and websocket features (where
  // those remain applicable), so generation-2 peers are never rejected for
  // "old" capability checks.
  if ((version >> 4) === 2) {
    return true;
  }
  return (version >> 4) === (minimum >> 4) && (version & 0x0f) >= (minimum & 0x0f);
}