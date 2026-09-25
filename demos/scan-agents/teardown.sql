-- Removes everything created by demos/scan-agents/setup.sql.
-- Only run this against the demo catalog.

DROP SCHEMA IF EXISTS airdemo.raw_dump CASCADE;
DROP SCHEMA IF EXISTS airdemo.meh_events CASCADE;
DROP SCHEMA IF EXISTS airdemo.good_orders CASCADE;
DROP CATALOG IF EXISTS airdemo CASCADE;
