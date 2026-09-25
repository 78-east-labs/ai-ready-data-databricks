-- AI-Ready Data (Databricks) demo: Scan + Agents
--
-- Seeds three schemas in one catalog with deliberate readiness gaps so a scan
-- ranks them differently and an agents assessment on the worst one has
-- something to fix. Run on a SQL warehouse as a user who can create schemas
-- in the target catalog. Replace `airdemo` with your catalog if needed.
--
--   good_orders    documented, keyed, tagged, CDF on, masked, row filter
--   meh_events     partial comments, no keys, no tags, CDF on
--   raw_dump       nothing declared, nulls, duplicates, PII in the clear
--
-- Teardown: demos/scan-agents/teardown.sql

CREATE CATALOG IF NOT EXISTS airdemo COMMENT 'AI-Ready Data demo catalog';
USE CATALOG airdemo;

-- ---------------------------------------------------------------- good_orders
CREATE SCHEMA IF NOT EXISTS good_orders COMMENT 'Curated orders mart used by the support agent';
USE SCHEMA good_orders;

CREATE TABLE IF NOT EXISTS customers (
  customer_id   BIGINT      NOT NULL COMMENT 'Surrogate key, stable across systems',
  email         STRING      NOT NULL COMMENT 'Primary contact email (PII, masked)',
  full_name     STRING               COMMENT 'Display name (PII, masked)',
  region        STRING      NOT NULL COMMENT 'Sales region code: NA, EMEA, APAC',
  created_at    TIMESTAMP   NOT NULL COMMENT 'Row creation time in the source CRM',
  CONSTRAINT customers_pk PRIMARY KEY (customer_id)
)
COMMENT 'One row per customer. Source: CRM nightly extract.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableRowTracking' = 'true');

CREATE TABLE IF NOT EXISTS orders (
  order_id      BIGINT        NOT NULL COMMENT 'Order number from the commerce platform',
  customer_id   BIGINT        NOT NULL COMMENT 'FK to customers.customer_id',
  order_total   DECIMAL(12,2) NOT NULL COMMENT 'Order total in USD, tax included',
  status        STRING        NOT NULL COMMENT 'One of: placed, shipped, delivered, cancelled',
  ordered_at    TIMESTAMP     NOT NULL COMMENT 'Order placement time (UTC)',
  CONSTRAINT orders_pk PRIMARY KEY (order_id),
  CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
)
COMMENT 'One row per order. Source: commerce platform CDC via Auto Loader.'
CLUSTER BY (customer_id, ordered_at)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableRowTracking' = 'true');

-- CHECK constraints are only accepted through ALTER TABLE on Databricks
ALTER TABLE orders ADD CONSTRAINT orders_status_chk CHECK (status IN ('placed','shipped','delivered','cancelled'));
ALTER TABLE orders ADD CONSTRAINT orders_total_chk  CHECK (order_total >= 0);

INSERT INTO customers VALUES
  (1, 'ana@example.com',  'Ana Ruiz',    'NA',   timestamp'2026-01-05 10:00:00'),
  (2, 'ben@example.com',  'Ben Okafor',  'EMEA', timestamp'2026-01-06 11:00:00'),
  (3, 'chen@example.com', 'Chen Wei',    'APAC', timestamp'2026-01-07 12:00:00');

INSERT INTO orders VALUES
  (100, 1, 120.50, 'delivered', timestamp'2026-09-01 09:00:00'),
  (101, 1,  35.00, 'shipped',   timestamp'2026-09-10 09:00:00'),
  (102, 2, 999.99, 'placed',    timestamp'2026-09-20 09:00:00'),
  (103, 3,  15.25, 'cancelled', timestamp'2026-09-21 09:00:00');

-- Tags (governance, SLA, purpose, provenance)
ALTER TABLE customers SET TAGS ('data_domain' = 'customer', 'sensitivity' = 'confidential',
  'source_system' = 'crm', 'collection_method' = 'batch_extract', 'freshness_sla_hours' = '24',
  'retention_days' = '2555', 'legal_basis' = 'contract', 'ai_allowed_purposes' = 'agents,analytics');
ALTER TABLE orders SET TAGS ('data_domain' = 'orders', 'sensitivity' = 'internal',
  'source_system' = 'commerce', 'collection_method' = 'cdc', 'freshness_sla_hours' = '4',
  'retention_days' = '2555', 'ai_allowed_purposes' = 'agents,analytics,training');
ALTER TABLE customers ALTER COLUMN email     SET TAGS ('pii' = 'email');
ALTER TABLE customers ALTER COLUMN full_name SET TAGS ('pii' = 'name');
ALTER TABLE orders    ALTER COLUMN order_total SET TAGS ('unit' = 'usd', 'glossary_term' = 'gross_order_value');

-- Column masks and a row filter
CREATE FUNCTION IF NOT EXISTS mask_email(v STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('pii_readers') THEN v
              ELSE concat(left(v, 1), '***@', split_part(v, '@', 2)) END;
CREATE FUNCTION IF NOT EXISTS mask_text(v STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('pii_readers') THEN v ELSE '***' END;
CREATE FUNCTION IF NOT EXISTS region_filter(r STRING) RETURNS BOOLEAN
  RETURN is_account_group_member('global_readers') OR r = 'NA';

ALTER TABLE customers ALTER COLUMN email     SET MASK mask_email;
ALTER TABLE customers ALTER COLUMN full_name SET MASK mask_text;
ALTER TABLE customers SET ROW FILTER region_filter ON (region);

-- ---------------------------------------------------------------- meh_events
CREATE SCHEMA IF NOT EXISTS meh_events;
USE SCHEMA meh_events;

CREATE TABLE IF NOT EXISTS page_views (
  view_id     STRING    COMMENT 'Event id from the web SDK',
  user_id     BIGINT,
  url         STRING,
  viewed_at   TIMESTAMP COMMENT 'Client timestamp, UTC',
  duration_ms BIGINT
)
COMMENT 'Web page view events'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO page_views VALUES
  ('e1', 1, '/home',     timestamp'2026-09-22 08:00:00', 1200),
  ('e2', 1, '/pricing',  timestamp'2026-09-22 08:01:00', 5400),
  ('e3', 2, '/home',     timestamp'2026-09-22 09:00:00', NULL),
  ('e3', 2, '/home',     timestamp'2026-09-22 09:00:00', NULL);

CREATE TABLE IF NOT EXISTS sessions (
  session_id  STRING,
  user_id     BIGINT,
  started_at  TIMESTAMP,
  ended_at    TIMESTAMP
);

INSERT INTO sessions VALUES
  ('s1', 1, timestamp'2026-09-22 08:00:00', timestamp'2026-09-22 08:10:00'),
  ('s2', 2, timestamp'2026-09-22 09:00:00', timestamp'2026-09-22 08:50:00');  -- ended before started

-- ---------------------------------------------------------------- raw_dump
CREATE SCHEMA IF NOT EXISTS raw_dump;
USE SCHEMA raw_dump;

CREATE TABLE IF NOT EXISTS contacts (
  id       STRING,
  email    STRING,
  phone    STRING,
  ssn      STRING,
  notes    STRING,
  payload  STRING
);

INSERT INTO contacts VALUES
  ('1', 'ana@example.com', '555-0100', '123-45-6789', 'vip',           '{"tier":"gold"}'),
  ('1', 'ana@example.com', '555-0100', '123-45-6789', 'vip',           '{"tier":"gold"}'),
  ('2', NULL,              NULL,       NULL,          'caf�',      '{"tier":'),
  ('3', 'not-an-email',    '555-0102', '000-00-0000', NULL,            'null');

CREATE TABLE IF NOT EXISTS export_2026_09 (
  c0 STRING, c1 STRING, c2 STRING, c3 STRING
);

INSERT INTO export_2026_09 VALUES ('a', '1', '2026-09-01', 'x'), ('b', 'two', 'yesterday', NULL);

-- Reset context
USE CATALOG airdemo;
USE SCHEMA good_orders;
