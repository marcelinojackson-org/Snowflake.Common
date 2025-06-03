import https from 'https';
import { URL } from 'url';
import snowflake from 'snowflake-sdk';
import type { Connection, ConnectionOptions, SnowflakeError } from 'snowflake-sdk';

export interface SnowflakeConnectionConfig {
  account?: string;
  username?: string;
  password?: string;
  privateKeyPath?: string;
  role?: string;
  logLevel?: LogLevel;
  warehouse?: string;
  database?: string;
  schema?: string;
}

interface ResolvedSnowflakeConnectionConfig {
  account: string;
  username: string;
  role: string;
  password?: string;
  privateKeyPath?: string;
  logLevel: LogLevel;
  warehouse?: string;
  database?: string;
  schema?: string;
}

type ExtendedConnection = Connection & {
  getId?: () => string;
  getSessionState?: () => {
    currentWarehouse?: string;
    currentDatabase?: string;
    currentSchema?: string;
    currentRole?: string;
  };
};

interface ConnectionContext {
  config: ResolvedSnowflakeConnectionConfig;
  accountIdentifier: string;
  debugLog: string[];
  verbose: boolean;
}

export interface SnowflakeConnectionResult {
  status: 'connected';
  connectionId: string;
  sessionId?: string;
  healthQueryId?: string;
  serverDateTime: string;
  defaultContext?: {
    warehouse?: string;
    database?: string;
    schema?: string;
    role?: string;
  };
  debugLog?: string[];
}

export type LogLevel = 'MINIMAL' | 'VERBOSE';

export interface CortexSearchParams {
  serviceName: string;
  query: string;
  limit?: number;
  filter?: string | Record<string, unknown>;
  accountUrl?: string;
  accessToken?: string;
  fields?: string[] | string;
  offset?: number;
  includeScores?: boolean | string;
  scoreThreshold?: number | string;
  reranker?: string;
  rankingProfile?: string;
}

export interface CortexSearchResult {
  status: 'success';
  httpStatus: number;
  serviceName: string;
  request: {
    query: string;
    limit: number;
    filter?: Record<string, unknown>;
    fields?: string[];
    offset?: number;
    includeScores?: boolean;
    scoreThreshold?: number;
    reranker?: string;
    rankingProfile?: string;
  };
  response: unknown;
}

const ROLE_ERROR = 'Missing Snowflake ROLE-set SNOWFLAKE_ROLE or input it, dummy!';

function resolveConfig(partial: SnowflakeConnectionConfig): ResolvedSnowflakeConnectionConfig {
  const account = partial.account ?? process.env.SNOWFLAKE_ACCOUNT;
  const username = partial.username ?? process.env.SNOWFLAKE_USER;
  const password = partial.password ?? process.env.SNOWFLAKE_PASSWORD;
  const privateKeyPath = partial.privateKeyPath ?? process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
  const role = partial.role ?? process.env.SNOWFLAKE_ROLE;
  const logLevel = normalizeLogLevel(partial.logLevel ?? process.env.SNOWFLAKE_LOG_LEVEL);
  const warehouse = partial.warehouse ?? process.env.SNOWFLAKE_WAREHOUSE;
  const database = partial.database ?? process.env.SNOWFLAKE_DATABASE;
  const schema = partial.schema ?? process.env.SNOWFLAKE_SCHEMA;

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

  return { account, username, password, privateKeyPath, role, logLevel, warehouse, database, schema };
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

function quoteIdentifier(value: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_$]*$/.test(value)) {
    return value;
  }
  return `"${value.replace(/"/g, '""')}"`;
}

function createConnectionContext(partialConfig: SnowflakeConnectionConfig) {
  const config = resolveConfig(partialConfig);
  const { identifier: accountIdentifier } = normalizeAccount(config.account);
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
  const connection = snowflake.createConnection(connectionOptions) as ExtendedConnection;

  return {
    connection,
    context: {
      config,
      accountIdentifier,
      debugLog,
      verbose
    }
  };
}

type HealthCheck = {
  serverTime: string;
  sessionId: string;
  queryId: string;
};

function fetchHealth(connection: ExtendedConnection, logLevel: LogLevel, debugLog: string[]): Promise<HealthCheck> {
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

export async function withSnowflakeConnection<T>(
  partialConfig: SnowflakeConnectionConfig = {},
  fn: (connection: ExtendedConnection, context: ConnectionContext) => Promise<T>
): Promise<T> {
  const { connection, context } = createConnectionContext(partialConfig);

  return new Promise((resolve, reject) => {
    connection.connect((err: SnowflakeError | undefined, conn: Connection | undefined) => {
      if (err) {
        return reject(err);
      }

      const safeConnection = (conn || connection) as ExtendedConnection;

      applyDesiredContext(safeConnection, context.config, context.debugLog)
        .then(() => Promise.resolve(fn(safeConnection, context)))
        .then((result) => {
          safeConnection.destroy((destroyErr?: SnowflakeError | null) => {
            if (destroyErr) {
              console.warn('Failed to close Snowflake connection cleanly:', destroyErr);
            }
            resolve(result);
          });
        })
        .catch((fnErr) => {
          safeConnection.destroy(() => reject(fnErr));
        });
    });
  });
}

async function applyDesiredContext(
  connection: ExtendedConnection,
  config: ResolvedSnowflakeConnectionConfig,
  debugLog: string[]
): Promise<void> {
  const statements: string[] = [];

  if (config.role) {
    statements.push(`use role ${quoteIdentifier(config.role)}`);
  }
  if (config.warehouse) {
    statements.push(`use warehouse ${quoteIdentifier(config.warehouse)}`);
  }
  if (config.database) {
    statements.push(`use database ${quoteIdentifier(config.database)}`);
  }
  if (config.schema) {
    statements.push(`use schema ${quoteIdentifier(config.schema)}`);
  }

  for (const sql of statements) {
    if (config.logLevel === 'VERBOSE') {
      debugLog.push(`[VERBOSE] Executing context SQL: ${sql}`);
    }
    await executeStatement(connection, sql);
  }
}

function executeStatement(connection: ExtendedConnection, sqlText: string): Promise<void> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      complete: (err: SnowflakeError | undefined) => {
        if (err) {
          return reject(err);
        }
        resolve();
      }
    });
  });
}

function buildDefaultContext(connection: ExtendedConnection): SnowflakeConnectionResult['defaultContext'] {
  const sessionState = typeof connection.getSessionState === 'function' ? connection.getSessionState() : undefined;
  if (!sessionState) {
    return undefined;
  }

  return {
    warehouse: sessionState.currentWarehouse,
    database: sessionState.currentDatabase,
    schema: sessionState.currentSchema,
    role: sessionState.currentRole
  };
}

export async function getSnowflakeConnection(
  partialConfig: SnowflakeConnectionConfig = {}
): Promise<SnowflakeConnectionResult> {
  return withSnowflakeConnection(partialConfig, async (connection, context) => {
    const connectionId = connection.getId ? connection.getId() : context.accountIdentifier;
    const health = await fetchHealth(connection, context.config.logLevel, context.debugLog).catch((timeErr) => {
      console.warn('Failed to fetch server time, falling back to local clock.', timeErr);
      return {
        serverTime: new Date().toISOString(),
        sessionId: connectionId,
        queryId: 'UNKNOWN'
      };
    });

    const summary: SnowflakeConnectionResult = {
      status: 'connected',
      connectionId,
      sessionId: health.sessionId,
      healthQueryId: health.queryId,
      serverDateTime: health.serverTime,
      ...(buildDefaultContext(connection) ? { defaultContext: buildDefaultContext(connection) } : {}),
      ...(context.verbose ? { debugLog: context.debugLog } : {})
    };

    return summary;
  });
}

export interface SnowflakeQueryResult {
  queryId: string;
  sessionId?: string;
  sqlText: string;
  rows: Array<Record<string, unknown>>;
  rowCount: number;
  defaultContext?: SnowflakeConnectionResult['defaultContext'];
}

async function executeSql(connection: ExtendedConnection, sqlText: string): Promise<SnowflakeQueryResult> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      complete: (executeErr: SnowflakeError | undefined, stmt: any, rows?: Array<Record<string, unknown>>) => {
        if (executeErr) {
          return reject(executeErr);
        }

        resolve({
          queryId: stmt && typeof stmt.getStatementId === 'function' ? stmt.getStatementId() : 'UNKNOWN',
          sessionId: connection.getId ? connection.getId() : undefined,
          sqlText,
          rows: rows ?? [],
          rowCount: rows?.length ?? 0,
          defaultContext: buildDefaultContext(connection)
        });
      }
    });
  });
}

export async function runSql(
  sqlText: string,
  partialConfig: SnowflakeConnectionConfig = {}
): Promise<SnowflakeQueryResult> {
  return withSnowflakeConnection(partialConfig, (connection) => executeSql(connection, sqlText));
}

export async function runSqlWithConnection(connection: Connection, sqlText: string): Promise<SnowflakeQueryResult> {
  return executeSql(connection as ExtendedConnection, sqlText);
}

export async function runCortexSearch(params: CortexSearchParams): Promise<CortexSearchResult> {
  const serviceName = params.serviceName?.trim();
  if (!serviceName) {
    throw new Error('Missing Cortex Search service name - provide serviceName or set SEARCH_SERVICE.');
  }

  const query = params.query?.trim();
  if (!query) {
    throw new Error('Missing Cortex Search query - provide query text or set SEARCH_QUERY.');
  }

  const limit = clampSearchLimit(params.limit);
  const filter = normalizeSearchFilter(params.filter);
  const fields = normalizeFields(params.fields);
  const offset = normalizeOffset(params.offset);
  const includeScores = normalizeBoolean(params.includeScores);
  const scoreThreshold = normalizeScoreThreshold(params.scoreThreshold);
  const { database, schema, service } = parseServiceName(serviceName);
  const { accountUrl, accessToken } = resolveAccountContext(params.accountUrl, params.accessToken);
  const endpoint = new URL(
    `/api/v2/databases/${database}/schemas/${schema}/cortex-search-services/${service}:query`,
    accountUrl
  );

  const payload: Record<string, unknown> = { query, limit };
  if (filter) {
    payload.filter = filter;
  }
  if (fields) {
    payload.fields = fields;
  }
  if (typeof offset === 'number') {
    payload.offset = offset;
  }
  if (typeof includeScores === 'boolean') {
    payload.include_scores = includeScores;
  }
  if (typeof scoreThreshold === 'number') {
    payload.score_threshold = scoreThreshold;
  }
  if (params.reranker) {
    payload.reranker = params.reranker.trim();
  }
  if (params.rankingProfile) {
    payload.ranking_profile = params.rankingProfile.trim();
  }

  const headers = {
    Authorization: `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
    Accept: 'application/json'
  };

  const response = await postJson(endpoint, JSON.stringify(payload), headers);
  const parsedBody = safeParseJson(response.body);

  if (response.status < 200 || response.status >= 300) {
    const message =
      (parsedBody && typeof parsedBody === 'object' && 'message' in parsedBody && parsedBody.message) ||
      `Cortex Search request failed with status ${response.status}`;
    const error = new Error(String(message));
    (error as Error & { status?: number; details?: unknown }).status = response.status;
    (error as Error & { status?: number; details?: unknown }).details = parsedBody ?? response.body;
    throw error;
  }

  return {
    status: 'success',
    httpStatus: response.status,
    serviceName,
    request: {
      query,
      limit,
      ...(filter ? { filter } : {}),
      ...(fields ? { fields } : {}),
      ...(typeof offset === 'number' ? { offset } : {}),
      ...(typeof includeScores === 'boolean' ? { includeScores } : {}),
      ...(typeof scoreThreshold === 'number' ? { scoreThreshold } : {}),
      ...(params.reranker ? { reranker: params.reranker.trim() } : {}),
      ...(params.rankingProfile ? { rankingProfile: params.rankingProfile.trim() } : {})
    },
    response: parsedBody
  };
}

function clampSearchLimit(limit?: number): number {
  if (typeof limit !== 'number' || Number.isNaN(limit) || limit <= 0) {
    return 3;
  }
  return Math.max(1, Math.floor(limit));
}

function normalizeSearchFilter(filter?: CortexSearchParams['filter']): Record<string, unknown> | undefined {
  if (filter === undefined || filter === null || filter === '') {
    return undefined;
  }

  if (typeof filter === 'string') {
    const parsed = JSON.parse(filter);
    if (typeof parsed !== 'object' || parsed === null) {
      throw new Error('SEARCH_FILTER must be a JSON object.');
    }
    return parsed as Record<string, unknown>;
  }

  if (typeof filter === 'object') {
    return filter as Record<string, unknown>;
  }

  throw new Error('Unsupported filter format. Provide a JSON string or object.');
}

function parseServiceName(serviceName: string): { database: string; schema: string; service: string } {
  const parts = serviceName.split('.');
  if (parts.length !== 3) {
    throw new Error('Cortex Search service name must be in the format DATABASE.SCHEMA.SERVICE');
  }

  const [database, schema, service] = parts.map((part) => part.trim());
  if (!database || !schema || !service) {
    throw new Error('Cortex Search service name contains empty segments');
  }

  return {
    database: database.toUpperCase(),
    schema: schema.toUpperCase(),
    service: service.toUpperCase()
  };
}

function resolveAccountContext(accountUrl?: string, accessToken?: string): { accountUrl: string; accessToken: string } {
  const rawUrl = accountUrl ?? process.env.SNOWFLAKE_ACCOUNT_URL;
  let normalizedUrl = rawUrl ? ensureHttpsUrl(rawUrl) : undefined;

  if (!normalizedUrl) {
    const account = process.env.SNOWFLAKE_ACCOUNT;
    if (!account) {
      throw new Error('Missing SNOWFLAKE_ACCOUNT_URL or SNOWFLAKE_ACCOUNT for Cortex Search.');
    }
    const { url } = normalizeAccount(account);
    normalizedUrl = `https://${url}`;
  }

  const token = accessToken ?? process.env.SNOWFLAKE_PAT ?? process.env.SNOWFLAKE_PASSWORD;
  if (!token) {
    throw new Error('Missing SNOWFLAKE_PAT or SNOWFLAKE_PASSWORD - provide credentials before running Cortex Search.');
  }

  return {
    accountUrl: normalizedUrl.replace(/\/$/, ''),
    accessToken: token
  };
}

function ensureHttpsUrl(url: string): string {
  const trimmed = url.trim();
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }
  return `https://${trimmed}`;
}

function postJson(url: URL, body: string, headers: Record<string, string>): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = https.request(
      url,
      {
        method: 'POST',
        headers: {
          'User-Agent': headers['User-Agent'] ?? 'Snowflake.CortexAI.Search/1.0',
          ...headers
        }
      },
      (res) => {
        let responseBody = '';
        res.on('data', (chunk) => {
          responseBody += chunk;
        });
        res.on('end', () => {
          resolve({
            status: res.statusCode ?? 0,
            body: responseBody
          });
        });
      }
    );

    req.on('error', (err) => reject(err));
    req.write(body);
    req.end();
  });
}

function safeParseJson(payload: string): unknown {
  if (!payload) {
    return undefined;
  }

  try {
    return JSON.parse(payload);
  } catch {
    return payload;
  }
}

function normalizeFields(fields?: CortexSearchParams['fields']): string[] | undefined {
  if (!fields) {
    return undefined;
  }

  if (Array.isArray(fields)) {
    const sanitized = fields.map((field) => field.trim()).filter((field) => field.length > 0);
    return sanitized.length > 0 ? sanitized : undefined;
  }

  const trimmed = fields.trim();
  if (!trimmed) {
    return undefined;
  }

  if (trimmed.startsWith('[')) {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed)) {
      throw new Error('SEARCH_FIELDS must be a comma-separated list or JSON array.');
    }
    return parsed.map((field) => String(field).trim()).filter((field) => field.length > 0);
  }

  return trimmed.split(',').map((field) => field.trim()).filter((field) => field.length > 0);
}

function normalizeOffset(offset?: number): number | undefined {
  if (offset === undefined || offset === null) {
    return undefined;
  }

  const value = Number(offset);
  if (Number.isNaN(value) || value < 0) {
    return 0;
  }

  return Math.floor(value);
}

function normalizeBoolean(value?: boolean | string): boolean | undefined {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  const normalized = value.trim().toLowerCase();
  if (['true', '1', 'yes', 'y'].includes(normalized)) {
    return true;
  }
  if (['false', '0', 'no', 'n'].includes(normalized)) {
    return false;
  }
  return undefined;
}

function normalizeScoreThreshold(value?: number | string): number | undefined {
  if (value === undefined || value === null || value === '') {
    return undefined;
  }

  const parsed = typeof value === 'number' ? value : Number(value);
  if (Number.isNaN(parsed)) {
    throw new Error('SEARCH_SCORE_THRESHOLD must be numeric.');
  }
  return parsed;
}
