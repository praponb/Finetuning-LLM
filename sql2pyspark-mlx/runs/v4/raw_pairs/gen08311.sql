WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        service,
        attr_key,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN flag_bit2 >= 1 THEN 'High' WHEN flag_bit2 >= 0 THEN 'Medium' ELSE 'Low' END AS flag_bit2_tier
    FROM flattened_attrs
    WHERE attr_key IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY attr_key ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_rank,
        STDDEV_SAMP(flag_bit2) OVER (PARTITION BY service, attr_key) AS service_stddev_samp_flag_bit2,
        SUM(flag_bit2) OVER (PARTITION BY service) AS service_sum_flag_bit2
    FROM cleaned
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(status_code_high_rank / SUM(status_code_high_rank) OVER (PARTITION BY service), 4) AS status_code_high_rank_share,
    MAX(service_stddev_samp_flag_bit2) OVER (PARTITION BY attr_key, service) AS attr_key_max_service_stddev_samp_flag_bit2,
    RANK() OVER w_running AS flag_bit2_rank
FROM with_windows
WINDOW w_running AS (PARTITION BY attr_key ORDER BY flag_bit2 DESC NULLS FIRST);
