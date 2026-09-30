WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM exploded
    QUALIFY RANK() OVER (PARTITION BY service ORDER BY status_code DESC) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(status_code) OVER w_part running_count_status_code,
        AVG(status_code) OVER w_part running_avg_status_code,
        SUM(status_code) OVER w_part running_sum_status_code
    FROM ranked
    WINDOW w_part AS (PARTITION BY attr_key ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(running_sum_status_code) OVER (PARTITION BY service ORDER BY status_code, id, attr_key) prev_running_sum_status_code,
        LAG(running_avg_status_code) OVER (PARTITION BY service ORDER BY status_code, id, attr_key) prev_running_avg_status_code,
        LEAD(running_avg_status_code) OVER (PARTITION BY service ORDER BY status_code, id, attr_key) next_running_avg_status_code,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code, id, attr_key) event_seq
    FROM with_windows
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        running_avg_status_code,
        running_sum_status_code,
        prev_running_avg_status_code,
        COALESCE(running_avg_status_code, 0) running_avg_status_code_filled
    FROM metrics
    WHERE service = 'search'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        host,
        level,
        attr_key,
        running_avg_status_code_filled
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code, id, attr_key) = 1
)
-- Grouped metrics
SELECT
    host,
    APPROX_PERCENTILE(running_avg_status_code_filled, 0.95) AS running_avg_status_code_filled_p95,
    ROUND(SUM(running_avg_status_code_filled) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS running_avg_status_code_filled_per_region,
    MAX(running_avg_status_code_filled) AS max_running_avg_status_code_filled,
    MAX(running_avg_status_code_filled) AS max_running_avg_status_code_filled_2
FROM latest
GROUP BY host
HAVING max_running_avg_status_code_filled_2 < 1000000;
