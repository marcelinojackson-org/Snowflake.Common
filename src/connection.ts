import snowflake from 'snowflake-sdk';
import type { Connection, ConnectionOptions, SnowflakeError } from 'snowflake-sdk';

export interface SnowflakeConnectionConfig {
  account?: string;
  username?: string;
  password?: string;
  privateKeyPath?: string;
  role?: string;
  logLevel?: LogLevel;
}

interface ResolvedSnowflakeConnectionConfig {
  account: string;
  username: string;
  role: string;
  password?: string;
  privateKeyPath?: string;
  logLevel: LogLevel;
}

export interface SnowflakeConnectionResult {
  status: 'connected';
  connectionId: string;
  serverDateTime: string;
  sessionId?: string;
  healthQueryId?: string;
  defaultContext?: {
    warehouse?: string;
    database?: string;
    schema?: string;
    role?: string;
  };
  debugLog?: string[];
}

export type LogLevel = 'MINIMAL' | 'VERBOSE';

const ROLE_ERROR = 'Missing Snowflake ROLE-set SNOWFLAKE_ROLE or input it, dummy!';

function resolveConfig(partial: SnowflakeConnectionConfig): ResolvedSnowflakeConnectionConfig {
  const account = partial.account ?? process.env.SNOWFLAKE_ACCOUNT;
  const username = partial.username ?? process.env.SNOWFLAKE_USER;
  const password = partial.password ?? process.env.SNOWFLAKE_PASSWORD;
  const privateKeyPath = partial.privateKeyPath ?? process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
  const role = partial.role ?? process.env.SNOWFLAKE_ROLE;
  const logLevel = normalizeLogLevel(partial.logLevel ?? process.env.SNOWFLAKE_LOG_LEVEL);

  if (!account) {
    throw new Error('Missing Snowflake account - set SNOWFLAKE_ACCOUNT or input it, dummy!');
  }

  if (!username) {
    throw new Error('Missing Snowflake user - set SNOWFLAKE_USER or input it, dummy!');
  }

  if (!password && !privateKeyPath) {
    throw new Error('Missing Snowflake auth - provide SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH, dummy!');
  }

  if (!role || role.trim().length === 0) {
    throw new Error(ROLE_ERROR);
  }

  return { account, username, password, privateKeyPath, role, logLevel };
}

function applySdkLogLevel(level: LogLevel): void {
  const sdkLevel = level === 'VERBOSE' ? 'DEBUG' : 'ERROR';
  const configure = (snowflake as unknown as { configure?: (opts: { logLevel: string }) => void }).configure;
  if (typeof configure === 'function') {
    configure({ logLevel: sdkLevel });
  }
}

function normalizeAccount(account: string): { identifier: string; url: string } {
  const trimmed = account.trim();
  const withoutProtocol = trimmed.replace(/^https?:\/\//i, '');
  const host = withoutProtocol.split('/')[0];
  const hostWithoutPort = host.split(':')[0];
  const identifier = hostWithoutPort.replace(/\.snowflakecomputing\.com$/i, '');

  if (!identifier) {
    throw new Error('Missing Snowflake account - set SNOWFLAKE_ACCOUNT or input it, dummy!');
  }

  return {
    identifier,
    url: `${identifier}.snowflakecomputing.com`
  };
}

function normalizeLogLevel(value: string | undefined): LogLevel {
  const upper = (value ?? 'MINIMAL').toString().trim().toUpperCase();
  return upper === 'VERBOSE' ? 'VERBOSE' : 'MINIMAL';
}

type HealthCheck = {
  serverTime: string;
  sessionId: string;
  queryId: string;
};

function fetchHealth(connection: Connection, logLevel: LogLevel, debugLog: string[]): Promise<HealthCheck> {
  const SQL = 'select current_timestamp() as server_time, current_session() as session_id';
  if (logLevel === 'VERBOSE') {
    debugLog.push(`[VERBOSE] Executing SQL to fetch server timestamp and session id: ${SQL}`);
  }

  return new Promise((resolve, reject) => {
    const options = {
      sqlText: SQL,
      complete: (err: SnowflakeError | undefined, stmt: Connection['execute'] extends (...args: any) => infer R ? R : any, rows?: Array<Record<string, unknown>>) => {
        if (err) {
          return reject(err);
        }

        const row = rows && rows[0];
        if (logLevel === 'VERBOSE') {
          debugLog.push(`[VERBOSE] Snowflake returned health row: ${JSON.stringify(row)}`);
        }

        const rawTime = row?.SERVER_TIME ?? row?.server_time ?? row?.SERVER_TIME?.toString?.();
        const sessionId = row?.SESSION_ID ?? row?.session_id;

        let serverTime = new Date().toISOString();
        if (typeof rawTime === 'string' && rawTime.length > 0) {
          serverTime = rawTime;
        } else if (rawTime && typeof rawTime !== 'string') {
          serverTime = String(rawTime);
        }

        const statementId = stmt && typeof (stmt as any).getStatementId === 'function'
          ? (stmt as any).getStatementId()
          : 'UNKNOWN';

        resolve({
          serverTime,
          sessionId: typeof sessionId === 'string' ? sessionId : connection.getId(),
          queryId: statementId
        });
      }
    };

    connection.execute(options);
  });
}

export async function getSnowflakeConnection(
  partialConfig: SnowflakeConnectionConfig = {}
): Promise<SnowflakeConnectionResult> {
  const config = resolveConfig(partialConfig);
  const { identifier: accountIdentifier, url: accountUrl } = normalizeAccount(config.account);
  const verbose = config.logLevel === 'VERBOSE';
  const debugLog: string[] = [];

  const connectionOptions: ConnectionOptions = {
    account: accountIdentifier,
    username: config.username,
    role: config.role,
    ...(config.password ? { password: config.password } : {}),
    ...(config.privateKeyPath ? { privateKeyPath: config.privateKeyPath } : {})
  };

  if (verbose) {
    const sanitized = {
      ...connectionOptions,
      password: connectionOptions.password ? '***redacted***' : undefined,
      privateKeyPath: connectionOptions.privateKeyPath ? '[provided]' : undefined
    };
    debugLog.push(`[VERBOSE] Initializing Snowflake connection with options: ${JSON.stringify(sanitized)}`);
  }

  applySdkLogLevel(config.logLevel);
  const connection = snowflake.createConnection(connectionOptions) as Connection & {
    getSessionState?: () => {
      currentWarehouse?: string;
      currentDatabase?: string;
      currentSchema?: string;
      currentRole?: string;
    };
  };

  return new Promise((resolve, reject) => {
    connection.connect((err: SnowflakeError | undefined, conn: Connection | undefined) => {
      if (err) {
        const snowflakeError = err as Error & { code?: string };
        const code = snowflakeError.code ?? 'UNKNOWN_CODE';
        console.error(`Snowflake error ${code}: ${snowflakeError.message}`);
        console.error(err.stack ?? '');
        return reject(err);
      }

      const safeConnection = (conn || connection) as Connection & {
        getId?: () => string;
        getSessionState?: () => {
          currentWarehouse?: string;
          currentDatabase?: string;
          currentSchema?: string;
          currentRole?: string;
        };
      };
      const connectionId = safeConnection.getId ? safeConnection.getId() : accountIdentifier;

      fetchHealth(safeConnection, config.logLevel, debugLog)
        .catch((timeErr) => {
          console.warn('Failed to fetch server time, falling back to local clock.', timeErr);
          return {
            serverTime: new Date().toISOString(),
            sessionId: undefined,
            queryId: 'UNKNOWN'
          };
        })
        .then((health) => {
          if (typeof safeConnection.destroy === 'function') {
            safeConnection.destroy((destroyErr?: SnowflakeError | null) => {
              if (destroyErr) {
                console.warn('Failed to close Snowflake connection cleanly:', destroyErr);
              }
            });
          }

          const defaultContext = typeof safeConnection.getSessionState === 'function'
            ? safeConnection.getSessionState()
            : undefined;

          const summary: SnowflakeConnectionResult = {
            status: 'connected',
            connectionId,
            serverDateTime: health.serverTime,
            sessionId: health.sessionId,
            healthQueryId: health.queryId,
            ...(defaultContext
              ? {
                  defaultContext: {
                    warehouse: defaultContext.currentWarehouse,
                    database: defaultContext.currentDatabase,
                    schema: defaultContext.currentSchema,
                    role: defaultContext.currentRole
                  }
                }
              : {}),
            ...(verbose ? { debugLog } : {})
          };

          resolve(summary);
        })
        .catch((timeErr) => reject(timeErr));
    });
  });
}
