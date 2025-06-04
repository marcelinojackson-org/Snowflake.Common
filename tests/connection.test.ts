import snowflake from 'snowflake-sdk';
import { getSnowflakeConnection, runSql, SnowflakeConnectionResult, __testHooks } from '../src/connection';

jest.mock('snowflake-sdk', () => {
  const createConnection = jest.fn();
  return { createConnection };
});

const mockedCreateConnection = snowflake.createConnection as jest.Mock;

const originalEnv = { ...process.env };

const HEALTH_SQL = 'select current_timestamp() as server_time, current_session() as session_id';

const buildFakeConnection = ({
  serverTime = '2025-11-08 12:00:00.000 +0000',
  failServerTime = false,
  queryRows = [{ CURRENT_USER: 'MARCELINO_J' }]
} = {}) => {
  const fakeStatement = { getStatementId: jest.fn(() => 'MOCK_QUERY_ID') };
  const fakeConnection: any = {
    execute: jest.fn(({ sqlText, complete }: any) => {
      if (sqlText === HEALTH_SQL) {
        if (failServerTime) {
          complete(new Error('boom'), null, undefined);
        } else {
          complete(null, null, [{ SERVER_TIME: serverTime, SESSION_ID: 'SESSION123' }]);
        }
      } else {
        complete(null, fakeStatement, queryRows);
      }
    }),
    destroy: jest.fn((cb?: (err?: Error | null) => void) => cb && cb()),
    getId: jest.fn(() => 'ABC123'),
    connect: jest.fn((cb: any) => cb(null, fakeConnection))
  };
  return fakeConnection;
};

let logSpy: jest.SpyInstance;
let warnSpy: jest.SpyInstance;

beforeEach(() => {
  mockedCreateConnection.mockReset();
  process.env = { ...originalEnv };
  logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
  warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterEach(() => {
  logSpy.mockRestore();
  warnSpy.mockRestore();
  process.env = { ...originalEnv };
});

describe('getSnowflakeConnection', () => {
  it('blows up when the role is missing', async () => {
    await expect(
      getSnowflakeConnection({
        account: 'myaccount',
        username: 'dummy',
        password: 'secret',
        role: ''
      })
    ).rejects.toThrow('Missing Snowflake ROLE-set SNOWFLAKE_ROLE or input it, dummy!');
  });

  it('blows up when env-based account is missing', async () => {
    delete process.env.SNOWFLAKE_ACCOUNT;
    process.env.SNOWFLAKE_USER = 'dummy';
    process.env.SNOWFLAKE_PASSWORD = 'secret';
    process.env.SNOWFLAKE_ROLE = 'DEV_ROLE';

    await expect(getSnowflakeConnection()).rejects.toThrow(
      'Missing Snowflake account - set SNOWFLAKE_ACCOUNT or input it, dummy!'
    );
  });

  it('blows up when env-based username is missing', async () => {
    process.env.SNOWFLAKE_ACCOUNT = 'myaccount';
    delete process.env.SNOWFLAKE_USER;
    process.env.SNOWFLAKE_PASSWORD = 'secret';
    process.env.SNOWFLAKE_ROLE = 'DEV_ROLE';

    await expect(getSnowflakeConnection()).rejects.toThrow(
      'Missing Snowflake user - set SNOWFLAKE_USER or input it, dummy!'
    );
  });

  it('blows up when no password or key path is provided', async () => {
    process.env.SNOWFLAKE_ACCOUNT = 'myaccount';
    process.env.SNOWFLAKE_USER = 'dummy';
    delete process.env.SNOWFLAKE_PASSWORD;
    delete process.env.SNOWFLAKE_PRIVATE_KEY_PATH;
    process.env.SNOWFLAKE_ROLE = 'DEV_ROLE';

    await expect(getSnowflakeConnection()).rejects.toThrow(
      'Missing Snowflake auth - provide SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH, dummy!'
    );
  });

  it('returns summary without logging when level is minimal', async () => {
    const fakeConnection = buildFakeConnection();
    mockedCreateConnection.mockReturnValue(fakeConnection);

    const result = await getSnowflakeConnection({
      account: 'myaccount',
      username: 'dummy',
      password: 'secret',
      role: 'DEV_ROLE'
    });

    expect(result.status).toBe('connected');
    expect(result.connectionId).toBe('ABC123');
    expect(result.serverDateTime).toBe('2025-11-08 12:00:00.000 +0000');
    expect(result.sessionId).toBeDefined();
    expect(result.healthQueryId).toBe('UNKNOWN');
    expect(result.debugLog).toBeUndefined();
    expect(logSpy).not.toHaveBeenCalled();
    expect(fakeConnection.execute).toHaveBeenCalledWith(
      expect.objectContaining({
        sqlText: expect.stringContaining('current_timestamp')
      })
    );
    expect(fakeConnection.destroy).toHaveBeenCalled();
  });

  it('falls back to ISO timestamp if fetching server time fails', async () => {
    const fakeConnection = buildFakeConnection({ failServerTime: true });
    mockedCreateConnection.mockReturnValue(fakeConnection);

    const result = await getSnowflakeConnection({
      account: 'https://srsibdn-ura06696.snowflakecomputing.com/',
      username: 'dummy',
      password: 'secret',
      role: 'DEV_ROLE'
    });

    expect(mockedCreateConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        account: 'srsibdn-ura06696'
      })
    );
    expect(result.status).toBe('connected');
    expect(result.connectionId).toBe('ABC123');
    expect(result.serverDateTime).toMatch(/T/);
    expect(console.warn).toHaveBeenCalledWith(
      'Failed to fetch server time, falling back to local clock.',
      expect.any(Error)
    );
  });

  it('emits verbose logs when log level is verbose', async () => {
    const fakeConnection = buildFakeConnection();
    mockedCreateConnection.mockReturnValue(fakeConnection);

    const result = await getSnowflakeConnection({
      account: 'myaccount',
      username: 'dummy',
      password: 'secret',
      role: 'DEV_ROLE',
      logLevel: 'VERBOSE'
    });

    expect(result.debugLog).toBeDefined();
    expect(result.debugLog?.some((entry: string) => entry.includes('Initializing Snowflake connection'))).toBe(true);
  });

  it('derives verbose log level from env var (case-insensitive)', async () => {
    const fakeConnection = buildFakeConnection();
    mockedCreateConnection.mockReturnValue(fakeConnection);
    process.env.SNOWFLAKE_LOG_LEVEL = 'verbose';

    const result = await getSnowflakeConnection({
      account: 'myaccount',
      username: 'dummy',
      password: 'secret',
      role: 'DEV_ROLE'
    });

    expect(result.debugLog?.some((entry: string) => entry.includes('Initializing Snowflake connection'))).toBe(true);
  });
});

describe('runSql', () => {
  it('returns rows and metadata', async () => {
    const fakeConnection = buildFakeConnection();
    mockedCreateConnection.mockReturnValue(fakeConnection);

    const result = await runSql('select current_user()', {
      account: 'myaccount',
      username: 'dummy',
      password: 'secret',
      role: 'DEV_ROLE'
    });

    expect(result.queryId).toBe('MOCK_QUERY_ID');
    expect(result.sessionId).toBe('ABC123');
    expect(result.rows).toEqual([{ CURRENT_USER: 'MARCELINO_J' }]);
    expect(result.rowCount).toBe(1);
    expect(fakeConnection.execute).toHaveBeenCalledWith(
      expect.objectContaining({ sqlText: 'select current_user()' })
    );
    expect(fakeConnection.destroy).toHaveBeenCalled();
  });
});

describe('Cortex Agent helpers', () => {
  const { parseSseEvents, validateAgentMessages } = __testHooks;

  it('parses SSE payloads into events', () => {
    const payload = [
      'event: response.status',
      'data: {"status":"planning"}',
      '',
      'event: response',
      'data: {"role":"assistant","content":[{"type":"text","text":"Hi"}]}',
      ''
    ].join('\n');

    const events = parseSseEvents(payload);
    expect(events).toHaveLength(2);
    expect(events[0]).toEqual({
      event: 'response.status',
      data: '{"status":"planning"}',
      raw: expect.any(String)
    });
    expect(events[1].event).toBe('response');
  });

  it('validates agent messages', () => {
    const sanitized = validateAgentMessages([
      {
        role: ' user ',
        content: [
          {
            type: 'text',
            text: 'Hello'
          }
        ]
      }
    ]);

    expect(sanitized[0].role).toBe('user');
  });

  it('throws when agent messages are invalid', () => {
    expect(() => validateAgentMessages([] as any)).toThrow('Cortex Agent messages array cannot be empty.');
    expect(() =>
      validateAgentMessages([
        {
          role: '',
          content: []
        }
      ] as any)
    ).toThrow(/missing role/);
  });
});
