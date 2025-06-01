import snowflake from 'snowflake-sdk';
import { getSnowflakeConnection } from '../src/connection';

jest.mock('snowflake-sdk', () => {
  const createConnection = jest.fn();
  return { createConnection };
});

const mockedCreateConnection = snowflake.createConnection as jest.Mock;

beforeEach(() => {
  mockedCreateConnection.mockReset();
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

  it('connects when all required inputs are present', async () => {
    const fakeConnection = { id: '123', connect: jest.fn((cb) => cb(null, { id: '123' })) };
    mockedCreateConnection.mockReturnValue(fakeConnection);

    await expect(
      getSnowflakeConnection({
        account: 'myaccount',
        username: 'dummy',
        password: 'secret',
        role: 'DEV_ROLE'
      })
    ).resolves.toEqual({ id: '123' });

    expect(mockedCreateConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        account: 'myaccount',
        username: 'dummy',
        role: 'DEV_ROLE'
      })
    );
    expect(fakeConnection.connect).toHaveBeenCalled();
  });
});
