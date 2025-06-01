import snowflake, { Connection, ConnectionOptions } from 'snowflake-sdk';

export interface SnowflakeConnectionConfig {
  account?: string;
  username?: string;
  password?: string;
  privateKeyPath?: string;
  role?: string;
}

interface ResolvedSnowflakeConnectionConfig {
  account: string;
  username: string;
  role: string;
  password?: string;
  privateKeyPath?: string;
}

const ROLE_ERROR = 'Missing Snowflake ROLE-set SNOWFLAKE_ROLE or input it, dummy!';

function resolveConfig(partial: SnowflakeConnectionConfig): ResolvedSnowflakeConnectionConfig {
  const account = partial.account ?? process.env.SNOWFLAKE_ACCOUNT;
  const username = partial.username ?? process.env.SNOWFLAKE_USER;
  const password = partial.password ?? process.env.SNOWFLAKE_PASSWORD;
  const privateKeyPath = partial.privateKeyPath ?? process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
  const role = partial.role ?? process.env.SNOWFLAKE_ROLE;

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

  return { account, username, password, privateKeyPath, role };
}

export async function getSnowflakeConnection(partialConfig: SnowflakeConnectionConfig = {}): Promise<Connection> {
  const config = resolveConfig(partialConfig);
  const accountIdentifier = config.account.endsWith('.snowflakecomputing.com')
    ? config.account.replace(/\.snowflakecomputing\.com$/, '')
    : config.account;
  const accountUrl = `${accountIdentifier}.snowflakecomputing.com`;

  const connectionOptions: ConnectionOptions = {
    account: accountIdentifier,
    username: config.username,
    role: config.role,
    ...(config.password ? { password: config.password } : {}),
    ...(config.privateKeyPath ? { privateKeyPath: config.privateKeyPath } : {})
  };

  const connection = snowflake.createConnection(connectionOptions);

  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        const snowflakeError = err as Error & { code?: string };
        const code = snowflakeError.code ?? 'UNKNOWN_CODE';
        console.error(`Snowflake error ${code}: ${snowflakeError.message}`);
        return reject(err);
      }

      console.log(`connected to ${accountUrl}`);
      resolve(conn as Connection);
    });
  });
}
