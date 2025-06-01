import snowflake from 'snowflake-sdk';
import { getSnowflakeConnection, SnowflakeConnectionResult } from '../src/connection';

jest.mock('snowflake-sdk', () => {
  const createConnection = jest.fn();
  return { createConnection };
});

const mockedCreateConnection = snowflake.createConnection as jest.Mock;

const originalEnv = { ...process.env };

const buildFakeConnection = ({
  serverTime = '2025-11-08 12:00:00.000 +0000',
  failServerTime = false
} = {}) => {
  const fakeConnection: any = {
    execute: jest.fn(({ complete }: any) => {
      if (failServerTime) {
        complete(new Error('boom'), null, undefined);
      } else {
        complete(null, null, [{ SERVER_TIME: serverTime }]);
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

  it('returns pretty JSON summary on success', async () => {
    const fakeConnection = buildFakeConnection();
    mockedCreateConnection.mockReturnValue(fakeConnection);

    const result = await getSnowflakeConnection({
      account: 'myaccount',
      username: 'dummy',
      password: 'secret',
      role: 'DEV_ROLE'
    });

    const expectedSummary: SnowflakeConnectionResult = {
      status: 'connected',
      connectionId: 'ABC123',
      serverDateTime: '2025-11-08 12:00:00.000 +0000'
    };

    expect(result).toEqual(expectedSummary);
    expect(console.log).toHaveBeenCalledWith(JSON.stringify(expectedSummary, null, 2));
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
});
