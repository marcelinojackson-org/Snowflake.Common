import snowflake, { Connection, ConnectionOptions } from 'snowflake-sdk';

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

function fetchServerTime(connection: Connection, logLevel: LogLevel): Promise<string> {
  const SQL = 'select current_timestamp() as server_time';
  if (logLevel === 'VERBOSE') {
    console.log('[VERBOSE] Executing SQL to fetch server timestamp:', SQL);
  }

  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: SQL,
      complete: (err, _stmt, rows) => {
        if (err) {
          return reject(err);
        }

        const row = rows && rows[0];
        if (logLevel === 'VERBOSE') {
          console.log('[VERBOSE] Snowflake returned server time row:', row);
        }

        const rawTime = row?.SERVER_TIME ?? row?.server_time ?? row?.SERVER_TIME?.toString?.();
        if (typeof rawTime === 'string' && rawTime.length > 0) {
          return resolve(rawTime);
        }

        if (rawTime && typeof rawTime !== 'string') {
          return resolve(String(rawTime));
        }

        return resolve(new Date().toISOString());
      }
    });
  });
}

export async function getSnowflakeConnection(
  partialConfig: SnowflakeConnectionConfig = {}
): Promise<SnowflakeConnectionResult> {
  const config = resolveConfig(partialConfig);
  const { identifier: accountIdentifier, url: accountUrl } = normalizeAccount(config.account);
  const verbose = config.logLevel === 'VERBOSE';

  const connectionOptions: ConnectionOptions = {
    account: accountIdentifier,
    username: config.username,
    role: config.role,
    ...(config.password ? { password: config.password } : {}),
    ...(config.privateKeyPath ? { privateKeyPath: config.privateKeyPath } : {})
  };

  if (verbose) {
    console.log('[VERBOSE] Initializing Snowflake connection with options:', {
      ...connectionOptions,
      password: connectionOptions.password ? '***redacted***' : undefined,
      privateKeyPath: connectionOptions.privateKeyPath ? '[provided]' : undefined
    });
  }

  const connection = snowflake.createConnection(connectionOptions);

  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        const snowflakeError = err as Error & { code?: string };
        const code = snowflakeError.code ?? 'UNKNOWN_CODE';
        console.error(`Snowflake error ${code}: ${snowflakeError.message}`);
        console.error(err.stack ?? '');
        return reject(err);
      }

      const safeConnection = (conn || connection) as Connection & { getId?: () => string };
      const connectionId = safeConnection.getId ? safeConnection.getId() : accountIdentifier;

      fetchServerTime(safeConnection, config.logLevel)
        .catch((timeErr) => {
          console.warn('Failed to fetch server time, falling back to local clock.', timeErr);
          return new Date().toISOString();
        })
        .then((serverDateTime) => {
          if (typeof safeConnection.destroy === 'function') {
            safeConnection.destroy((destroyErr) => {
              if (destroyErr) {
                console.warn('Failed to close Snowflake connection cleanly:', destroyErr);
              }
            });
          }

          const summary: SnowflakeConnectionResult = {
            status: 'connected',
            connectionId,
            serverDateTime
          };

          if (verbose) {
            console.log('[VERBOSE] Snowflake connection summary:', summary);
          }
          console.log(JSON.stringify(summary, null, 2));
          console.log(`connected to ${accountUrl}`);
          resolve(summary);
        })
        .catch((timeErr) => reject(timeErr));
    });
  });
}
