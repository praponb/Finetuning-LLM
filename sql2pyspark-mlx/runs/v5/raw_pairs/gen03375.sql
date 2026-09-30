WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(attr_key) AS attr_key_upper
    FROM lines
    WHERE log_ts > TIMESTAMP '2025-06-01 06:00:00'
      AND attr_key IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(attr_key) AS attr_key_lower,
        DATE_TRUNC('year', log_ts) AS log_ts_period
    FROM valid
    WHERE attr_key IS NOT NULL
      AND TO_DATE(log_ts) >= '2025-10-05'
),
valid_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        log_ts,
        attr_key,
        value,
        attr_key_upper,
        attr_key_lower,
        log_ts_period,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts
    FROM filtered
    WHERE attr_key IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        attr_key,
        value,
        attr_key_lower,
        log_ts_period,
        days_since_log_ts,
        CASE WHEN attr_key IN ('region', 'trace') THEN 'Priority' ELSE 'Other' END AS attr_key_group,
        COALESCE(attr_key, 'NONE') AS attr_key_filled
    FROM valid_app_logs
    WHERE log_ts > TIMESTAMP '2025-06-01 13:00:00'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        value,
        attr_key_lower,
        days_since_log_ts,
        attr_key_group,
        attr_key_filled,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY days_since_log_ts DESC) AS days_since_log_ts_percent_rank
    FROM base
)
SELECT
    value,
    attr_key_lower,
    days_since_log_ts,
    attr_key_group,
    attr_key_filled
FROM with_windows
ORDER BY days_since_log_ts DESC NULLS LAST;
