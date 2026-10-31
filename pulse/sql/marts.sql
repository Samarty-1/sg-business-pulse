-- Tables a business reader can use directly. Rebuilt from scratch on every run.

-- New registrations per month, by industry and legal form.
CREATE OR REPLACE TABLE mart_formations_monthly AS
SELECT
    date_trunc('month', registration_date)::DATE AS month,
    industry,
    entity_kind,
    count(*)                                     AS registrations,
    count(*) FILTER (WHERE is_food_and_beverage) AS fnb_registrations
FROM stg_entities
WHERE registration_date >= DATE '2000-01-01'
  AND entity_kind IN ('Company', 'Sole proprietor or partnership', 'LLP')
GROUP BY ALL
ORDER BY ALL;

-- "Of the businesses registered in year Y, what share are still live today?"
-- A cohort table, so young cohorts look healthier simply because they are young:
-- compare cohorts at the same age, not the same calendar year.
CREATE OR REPLACE TABLE mart_cohort_survival AS
WITH base AS (
    SELECT
        registration_year,
        CASE WHEN is_food_and_beverage THEN 'Food and beverage' ELSE industry END AS industry,
        entity_kind,
        status_group,
        year(snapshot_date) - registration_year AS age_years
    FROM stg_entities
    WHERE registration_year BETWEEN 2005 AND year(snapshot_date) - 1
      AND entity_kind IN ('Company', 'Sole proprietor or partnership')
      AND status_group <> 'converted'
)
SELECT
    registration_year,
    industry,
    entity_kind,
    any_value(age_years)                              AS age_years,
    count(*)                                          AS registered,
    count(*) FILTER (WHERE status_group = 'live')     AS still_live,
    round(count(*) FILTER (WHERE status_group = 'live') / count(*), 4) AS share_live
FROM base
GROUP BY registration_year, industry, entity_kind
ORDER BY ALL;

-- Live businesses by postal sector and industry: a market-density table for site
-- selection. Registered address, which is not always where the business trades.
CREATE OR REPLACE TABLE mart_live_by_sector AS
SELECT
    postal_sector,
    CASE WHEN is_food_and_beverage THEN 'Food and beverage' ELSE industry END AS industry,
    count(*) AS live_entities
FROM stg_entities
WHERE status_group = 'live' AND postal_code_valid
GROUP BY ALL
ORDER BY ALL;

-- Month-on-month changes detected by comparing releases (empty until two releases exist).
CREATE OR REPLACE TABLE mart_status_changes AS
WITH v AS (
    SELECT
        uen, valid_from, change_type, status,
        lag(status) OVER (PARTITION BY uen ORDER BY valid_from) AS prev_status
    FROM entity_history
)
SELECT
    valid_from                 AS release_date,
    change_type,
    status_group(prev_status)  AS from_group,
    CASE WHEN change_type = 'removed' THEN 'removed' ELSE status_group(status) END AS to_group,
    count(*)                   AS entities
FROM v
WHERE change_type IN ('new', 'changed', 'removed', 'reappeared')
  -- a rename or a move is a change, but not a change of status
  AND (change_type <> 'changed' OR prev_status IS DISTINCT FROM status)
GROUP BY ALL
ORDER BY ALL;

-- SingStat indices, long format with year-on-year change.
CREATE OR REPLACE TABLE mart_sales_indices AS
-- Joined on the calendar month a year earlier, not "12 rows back", so a gap in a
-- series can never pair the wrong months.
SELECT
    c.table_name,
    c.series,
    c.month,
    c.value,
    round(c.value / p.value - 1, 4) AS yoy_change
FROM stg_singstat c
LEFT JOIN stg_singstat p
       ON p.table_name = c.table_name
      AND p.series = c.series
      AND p.month = c.month - INTERVAL 12 MONTH
ORDER BY ALL;
