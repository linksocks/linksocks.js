import { gzip, ungzip } from "pako";

// Protocol version
export const PROTOCOL_VERSION = 0x01;

// Binary message types
export const enum BinaryType {
  Auth = 0x01,
  AuthResponse = 0x02,
  Connect = 0x03,
  Data = 0x04,
  ConnectResponse = 0x05,
  Disconnect = 0x06,
  Connector = 0x07,
  ConnectorResponse = 0x08,
  Log = 0x09,
  Partners = 0x0A,
}

// Protocol types
export const enum BinaryProtocol {
  TCP = 0x01,
  UDP = 0x02,
}

// Binary connector operations
export const enum BinaryConnectorOperation {
  Add = 0x01,
  Remove = 0x02,
}

// Type strings
export const enum MessageType {
  Auth = "auth",
  AuthResponse = "auth_response",
  Connect = "connect",
  Data = "data",
  ConnectResponse = "connect_response",
  Disconnect = "disconnect",
  Connector = "connector",
  ConnectorResponse = "connector_response",
  Log = "log",
  Partners = "partners",
}

// Compression flags
export const enum DataCompression {
  None = 0x00,
  Gzip = 0x01,
}

// Base interface for all messages
export interface BaseMessage {
  getType(): string;
}

// Message interfaces
export interface AuthMessage extends BaseMessage {
  token: string;
  reverse: boolean;
  instance: string;
}

export interface AuthResponseMessage extends BaseMessage {
  success: boolean;
  token?: string;
  error?: string;
}

export interface ConnectMessage extends BaseMessage {
  protocol: string;
  address?: string;
  port?: number;
  channelId: string;
}

export interface ConnectResponseMessage extends BaseMessage {
  success: boolean;
  error?: string;
  channelId: string;
}

export interface DataMessage extends BaseMessage {
  protocol: string;
  channelId: string;
  data: Uint8Array;
  compression: DataCompression;
  address?: string;
  port?: number;
  targetAddr?: string;
  targetPort?: number;
}

export interface DisconnectMessage extends BaseMessage {
  channelId: string;
}

export interface ConnectorMessage extends BaseMessage {
  channelId: string;
  connectorToken: string;
  operation: string;
}

export interface ConnectorResponseMessage extends BaseMessage {
  success: boolean;
  error?: string;
  channelId: string;
  connectorToken?: string;
}

export interface LogMessage extends BaseMessage {
  level: string;
  msg: string;
}

export interface PartnersMessage extends BaseMessage {
  count: number;
}

// Helper functions
function protocolToBytes(protocol: string): number {
  switch (protocol) {
    case "tcp":
      return BinaryProtocol.TCP;
    case "udp":
      return BinaryProtocol.UDP;
    default:
      return 0;
  }
}

function bytesToProtocol(b: number): string {
  switch (b) {
    case BinaryProtocol.TCP:
      return "tcp";
    case BinaryProtocol.UDP:
      return "udp";
    default:
      return "";
  }
}

function uuidToBytes(uuid: string): Uint8Array {
  const hex = uuid.replace(/-/g, "");
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return bytes;
}

function bytesToUUID(b: Uint8Array): string {
  if (b.length !== 16) return "";
  const hex = Array.from(b)
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function operationToBytes(operation: string): number {
  switch (operation) {
    case "add":
      return BinaryConnectorOperation.Add;
    case "remove":
      return BinaryConnectorOperation.Remove;
    default:
      return 0;
  }
}

function bytesToOperation(b: number): string {
  switch (b) {
    case BinaryConnectorOperation.Add:
      return "add";
    case BinaryConnectorOperation.Remove:
      return "remove";
    default:
      return "";
  }
}

// Helper function to concatenate Uint8Arrays
function concatUint8Arrays(...arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((acc, arr) => acc + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

export function packMessage(msg: BaseMessage): Uint8Array {
  // Start with version
  const header = new Uint8Array([PROTOCOL_VERSION]);

  switch (msg.getType()) {
    case MessageType.Auth: {
      const authMsg = msg as AuthMessage;
      const tokenBytes = new TextEncoder().encode(authMsg.token);
      const instanceBytes = uuidToBytes(authMsg.instance);
      return concatUint8Arrays(
        header,
        new Uint8Array([
          BinaryType.Auth,
          tokenBytes.length,
          ...tokenBytes,
          authMsg.reverse ? 1 : 0,
          ...instanceBytes,
        ]),
      );
    }

    case MessageType.AuthResponse: {
      const authRespMsg = msg as AuthResponseMessage;
      const payload = [BinaryType.AuthResponse, authRespMsg.success ? 1 : 0];
      if (authRespMsg.success && authRespMsg.token) {
        const tokenBytes = new TextEncoder().encode(authRespMsg.token);
        payload.push(tokenBytes.length, ...tokenBytes);
      } else if (!authRespMsg.success && authRespMsg.error) {
        const errorBytes = new TextEncoder().encode(authRespMsg.error);
        payload.push(errorBytes.length, ...errorBytes);
      }
      return concatUint8Arrays(header, new Uint8Array(payload));
    }

    case MessageType.Connect: {
      const connectMsg = msg as ConnectMessage;
      const channelIdBytes = uuidToBytes(connectMsg.channelId);
      const payload = [
        BinaryType.Connect,
        protocolToBytes(connectMsg.protocol),
        ...channelIdBytes,
      ];

      if (
        connectMsg.protocol === "tcp" &&
        connectMsg.address &&
        connectMsg.port
      ) {
        const addrBytes = new TextEncoder().encode(connectMsg.address);
        payload.push(
          addrBytes.length,
          ...addrBytes,
          (connectMsg.port >> 8) & 0xff,
          connectMsg.port & 0xff,
        );
      }
      return concatUint8Arrays(header, new Uint8Array(payload));
    }

    case MessageType.Data: {
      const dataMsg = msg as DataMessage;
      const channelIdBytes = uuidToBytes(dataMsg.channelId);
      let compressedData = dataMsg.data;

      if (dataMsg.compression === DataCompression.Gzip) {
        compressedData = gzip(dataMsg.data);
      }

      const payload = [
        BinaryType.Data,
        protocolToBytes(dataMsg.protocol),
        ...channelIdBytes,
        dataMsg.compression,
        (compressedData.length >> 24) & 0xff,
        (compressedData.length >> 16) & 0xff,
        (compressedData.length >> 8) & 0xff,
        compressedData.length & 0xff,
      ];

      const dataArray = concatUint8Arrays(
        new Uint8Array(payload),
        compressedData,
      );

      if (dataMsg.protocol === "udp") {
        const addrBytes = new TextEncoder().encode(dataMsg.address || "");
        const targetAddrBytes = new TextEncoder().encode(
          dataMsg.targetAddr || "",
        );
        const udpInfo = new Uint8Array([
          addrBytes.length,
          ...addrBytes,
          ((dataMsg.port || 0) >> 8) & 0xff,
          (dataMsg.port || 0) & 0xff,
          targetAddrBytes.length,
          ...targetAddrBytes,
          ((dataMsg.targetPort || 0) >> 8) & 0xff,
          (dataMsg.targetPort || 0) & 0xff,
        ]);
        return concatUint8Arrays(header, dataArray, udpInfo);
      }

      return concatUint8Arrays(header, dataArray);
    }

    case MessageType.ConnectResponse: {
      const respMsg = msg as ConnectResponseMessage;
      const channelIdBytes = uuidToBytes(respMsg.channelId);
      const payload = [
        BinaryType.ConnectResponse,
        respMsg.success ? 1 : 0,
        ...channelIdBytes,
      ];

      if (!respMsg.success && respMsg.error) {
        const errorBytes = new TextEncoder().encode(respMsg.error);
        payload.push(errorBytes.length, ...errorBytes);
      }
      return concatUint8Arrays(header, new Uint8Array(payload));
    }

    case MessageType.Disconnect: {
      const disconnectMsg = msg as DisconnectMessage;
      const channelIdBytes = uuidToBytes(disconnectMsg.channelId);
      return concatUint8Arrays(
        header,
        new Uint8Array([BinaryType.Disconnect, ...channelIdBytes]),
      );
    }

    case MessageType.Connector: {
      const connectorMsg = msg as ConnectorMessage;
      const channelIdBytes = uuidToBytes(connectorMsg.channelId);
      const tokenBytes = new TextEncoder().encode(connectorMsg.connectorToken);
      return concatUint8Arrays(
        header,
        new Uint8Array([
          BinaryType.Connector,
          ...channelIdBytes,
          tokenBytes.length,
          ...tokenBytes,
          operationToBytes(connectorMsg.operation),
        ]),
      );
    }

    case MessageType.ConnectorResponse: {
      const respMsg = msg as ConnectorResponseMessage;
      const channelIdBytes = uuidToBytes(respMsg.channelId);
      const payload = [
        BinaryType.ConnectorResponse,
        ...channelIdBytes,
        respMsg.success ? 1 : 0,
      ];

      if (!respMsg.success && respMsg.error) {
        const errorBytes = new TextEncoder().encode(respMsg.error);
        payload.push(errorBytes.length, ...errorBytes);
      } else if (respMsg.connectorToken) {
        const tokenBytes = new TextEncoder().encode(respMsg.connectorToken);
        payload.push(tokenBytes.length, ...tokenBytes);
      }
      return concatUint8Arrays(header, new Uint8Array(payload));
    }

    case MessageType.Log: {
      const logMsg = msg as LogMessage;
      const jsonData = new TextEncoder().encode(JSON.stringify(logMsg));
      const payload = [
        BinaryType.Log,
        (jsonData.length >> 24) & 0xff,
        (jsonData.length >> 16) & 0xff,
        (jsonData.length >> 8) & 0xff,
        jsonData.length & 0xff,
      ];
      return concatUint8Arrays(header, new Uint8Array(payload), jsonData);
    }

    case MessageType.Partners: {
      const partnersMsg = msg as PartnersMessage;
      const jsonData = new TextEncoder().encode(JSON.stringify(partnersMsg));
      const payload = [
        BinaryType.Partners,
        (jsonData.length >> 24) & 0xff,
        (jsonData.length >> 16) & 0xff,
        (jsonData.length >> 8) & 0xff,
        jsonData.length & 0xff,
      ];
      return concatUint8Arrays(header, new Uint8Array(payload), jsonData);
    }

    default:
      throw new Error("Unsupported message type for binary serialization");
  }
}

export function parseMessage(data: Uint8Array): BaseMessage {
  if (data.length < 2) {
    throw new Error("Message too short");
  }

  const version = data[0];
  if (version !== PROTOCOL_VERSION) {
    throw new Error(`Unsupported protocol version: ${version}`);
  }

  const msgType = data[1];
  const payload = data.slice(2);

  switch (msgType) {
    case BinaryType.Auth: {
      if (payload.length < 1) {
        throw new Error("Invalid auth message");
      }
      const tokenLen = payload[0];
      if (payload.length < 1 + tokenLen + 1 + 16) {
        throw new Error("Invalid auth message length");
      }
      const token = new TextDecoder().decode(payload.slice(1, 1 + tokenLen));
      const reverse = Boolean(payload[1 + tokenLen]);
      const instance = bytesToUUID(
        payload.slice(1 + tokenLen + 1, 1 + tokenLen + 1 + 16),
      );
      return {
        token,
        reverse,
        instance,
        getType: () => MessageType.Auth,
      } as AuthMessage;
    }

    case BinaryType.AuthResponse: {
      if (payload.length < 1) {
        throw new Error("Invalid auth response message");
      }
      const success = Boolean(payload[0]);
      const msg: AuthResponseMessage = {
        success,
        getType: () => MessageType.AuthResponse,
      };
      if (payload.length > 1) {
        const strLen = payload[1];
        if (payload.length < 2 + strLen) {
          throw new Error("Invalid auth response data length");
        }
        if (success) {
          msg.token = new TextDecoder().decode(payload.slice(2, 2 + strLen));
        } else {
          msg.error = new TextDecoder().decode(payload.slice(2, 2 + strLen));
        }
      }
      return msg;
    }

    case BinaryType.Connect: {
      if (payload.length < 17) {
        throw new Error("Invalid connect message");
      }
      const protocol = bytesToProtocol(payload[0]);
      const channelId = bytesToUUID(payload.slice(1, 17));
      const msg: ConnectMessage = {
        protocol,
        channelId,
        getType: () => MessageType.Connect,
      };

      if (protocol === "tcp" && payload.length > 17) {
        const addrLen = payload[17];
        if (payload.length < 18 + addrLen + 2) {
          throw new Error("Invalid tcp connect message length");
        }
        msg.address = new TextDecoder().decode(payload.slice(18, 18 + addrLen));
        msg.port = (payload[18 + addrLen] << 8) | payload[18 + addrLen + 1];
      }
      return msg;
    }

    case BinaryType.Data: {
      if (payload.length < 22) {
        throw new Error("Invalid data message");
      }
      const protocol = bytesToProtocol(payload[0]);
      const channelId = bytesToUUID(payload.slice(1, 17));
      const compression = payload[17] as DataCompression;
      const dataLen =
        (payload[18] << 24) |
        (payload[19] << 16) |
        (payload[20] << 8) |
        payload[21];

      if (payload.length < 22 + dataLen) {
        throw new Error("Invalid data message length");
      }

      let data = payload.slice(22, 22 + dataLen);
      if (compression === DataCompression.Gzip) {
        data = ungzip(data) as Uint8Array<ArrayBuffer>;
      }

      const msg: DataMessage = {
        protocol,
        channelId,
        compression,
        data,
        getType: () => MessageType.Data,
      };

      if (protocol === "udp") {
        let offset = 22 + dataLen;
        if (payload.length < offset + 1) {
          throw new Error("Invalid udp data message");
        }
        const addrLen = payload[offset];
        offset++;
        if (payload.length < offset + addrLen + 2) {
          throw new Error("Invalid udp data message length");
        }
        msg.address = new TextDecoder().decode(
          payload.slice(offset, offset + addrLen),
        );
        offset += addrLen;
        msg.port = (payload[offset] << 8) | payload[offset + 1];
        offset += 2;

        if (payload.length < offset + 1) {
          throw new Error("Invalid udp data target address");
        }
        const targetAddrLen = payload[offset];
        offset++;
        if (payload.length < offset + targetAddrLen + 2) {
          throw new Error("Invalid udp data target address length");
        }
        msg.targetAddr = new TextDecoder().decode(
          payload.slice(offset, offset + targetAddrLen),
        );
        offset += targetAddrLen;
        msg.targetPort = (payload[offset] << 8) | payload[offset + 1];
      }
      return msg;
    }

    case BinaryType.ConnectResponse: {
      if (payload.length < 17) {
        throw new Error("Invalid connect response message");
      }
      const success = Boolean(payload[0]);
      const channelId = bytesToUUID(payload.slice(1, 17));
      const msg: ConnectResponseMessage = {
        success,
        channelId,
        getType: () => MessageType.ConnectResponse,
      };
      if (!success && payload.length > 17) {
        const errorLen = payload[17];
        if (payload.length < 18 + errorLen) {
          throw new Error("Invalid connect response error length");
        }
        msg.error = new TextDecoder().decode(payload.slice(18, 18 + errorLen));
      }
      return msg;
    }

    case BinaryType.Disconnect: {
      if (payload.length < 16) {
        throw new Error("Invalid disconnect message");
      }
      return {
        channelId: bytesToUUID(payload.slice(0, 16)),
        getType: () => MessageType.Disconnect,
      } as DisconnectMessage;
    }

    case BinaryType.Connector: {
      if (payload.length < 16) {
        throw new Error("Invalid connector message");
      }
      const channelId = bytesToUUID(payload.slice(0, 16));
      if (payload.length < 17) {
        throw new Error("Invalid connector token length");
      }
      const tokenLen = payload[16];
      if (payload.length < 17 + tokenLen + 1) {
        throw new Error("Invalid connector message length");
      }
      const connectorToken = new TextDecoder().decode(
        payload.slice(17, 17 + tokenLen),
      );
      const operation = bytesToOperation(payload[17 + tokenLen]);
      return {
        channelId,
        connectorToken,
        operation,
        getType: () => MessageType.Connector,
      } as ConnectorMessage;
    }

    case BinaryType.ConnectorResponse: {
      if (payload.length < 17) {
        throw new Error("Invalid connector response message");
      }
      const channelId = bytesToUUID(payload.slice(0, 16));
      const success = Boolean(payload[16]);
      const msg: ConnectorResponseMessage = {
        success,
        channelId,
        getType: () => MessageType.ConnectorResponse,
      };

      if (payload.length > 17) {
        const tokenLen = payload[17];
        if (payload.length < 18 + tokenLen) {
          throw new Error("Invalid connector response token length");
        }
        if (!success) {
          msg.error = new TextDecoder().decode(
            payload.slice(18, 18 + tokenLen),
          );
        } else {
          msg.connectorToken = new TextDecoder().decode(
            payload.slice(18, 18 + tokenLen),
          );
        }
      }
      return msg;
    }

    case BinaryType.Log: {
      if (payload.length < 4) {
        throw new Error("Invalid log message");
      }
      const dataLen = (payload[0] << 24) | (payload[1] << 16) | (payload[2] << 8) | payload[3];
      if (payload.length < 4 + dataLen) {
        throw new Error("Invalid log message length");
      }
      const jsonData = new TextDecoder().decode(payload.slice(4, 4 + dataLen));
      const parsed = JSON.parse(jsonData);
      return {
        ...parsed,
        getType: () => MessageType.Log,
      } as LogMessage;
    }

    case BinaryType.Partners: {
      if (payload.length < 4) {
        throw new Error("Invalid partners message");
      }
      const dataLen = (payload[0] << 24) | (payload[1] << 16) | (payload[2] << 8) | payload[3];
      if (payload.length < 4 + dataLen) {
        throw new Error("Invalid partners message length");
      }
      const jsonData = new TextDecoder().decode(payload.slice(4, 4 + dataLen));
      const parsed = JSON.parse(jsonData);
      return {
        ...parsed,
        getType: () => MessageType.Partners,
      } as PartnersMessage;
    }

    default:
      throw new Error(`Unknown binary message type: ${msgType}`);
  }
}
