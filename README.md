# Snowflake.Common

A tiny TypeScript helper that screams when Snowflake configuration is missing and logs exactly why a connection fails. Perfect for reuse inside GitHub Actions.

## Quick start

```bash
cd Snowflake.Common
npm install
npm test
npm run build
```

## Environment variables

- `SNOWFLAKE_ACCOUNT` – Account identifier like `myaccount` or `myaccount.snowflakecomputing.com`
- `SNOWFLAKE_USER` – Username
- `SNOWFLAKE_PASSWORD` or `SNOWFLAKE_PRIVATE_KEY_PATH` – One authentication path is required
- `SNOWFLAKE_ROLE` – Mandatory role; the helper yells if it is blank
- `SNOWFLAKE_LOG_LEVEL` – Optional (`MINIMAL` default, set `VERBOSE` for extra connection detail)

## API

```ts
import { getSnowflakeConnection } from '@marcelinojackson-org/snowflake-common';

const summary = await getSnowflakeConnection({
  account: 'myaccount',
  username: 'ME',
  password: 'super-secret',
  role: 'ACCOUNTADMIN',
  logLevel: 'VERBOSE'
});

// summary === {
//   status: 'connected',
//   connectionId: 'XYZ',
//   serverDateTime: '2025-01-01 00:00:00.000 +0000'
// }
```

The helper prints the summary as pretty JSON, fetches `current_timestamp()` from Snowflake for traceability, and rethrows detailed Snowflake error codes on failure.
