WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, v
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        attr_key,
        v,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE attr_key = 'version'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(attr_key, 1, 4) AS attr_key_prefix,
        status_code_high * flag_bit2 AS status_code_high_times_flag_bit2
    FROM parsed
    WHERE attr_key IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_grp AS status_code_high_times_flag_bit2_cume_dist,
        LEAD(status_code_high_times_flag_bit2, 1) OVER w_hist AS next_status_code_high_times_flag_bit2,
        LAG(status_code_high, 2, 0) OVER w_hist AS prev_status_code_high,
        AVG(status_code_high) OVER w_part AS status_code_high_avg_6,
        AVG(status_code_high_times_flag_bit2) OVER (PARTITION BY attr_key ORDER BY status_code_high_times_flag_bit2 ASC, id ASC, attr_key ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_code_high_times_flag_bit2_avg_2_prior
    FROM filtered
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY status_code_high_times_flag_bit2 ASC), w_hist AS (PARTITION BY attr_key ORDER BY status_code_high ASC, id ASC, attr_key ASC), w_part AS (PARTITION BY attr_key ORDER BY status_code_high_times_flag_bit2 ASC, id ASC, attr_key ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(status_code_high_times_flag_bit2_avg_2_prior, 1) AS status_code_high_times_flag_bit2_avg_2_prior_rounded
    FROM metrics
    WHERE status_code_high_times_flag_bit2_cume_dist >= 0.54
),
metrics_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        attr_key,
        v,
        flag_bit2,
        status_code_high,
        attr_key_prefix,
        status_code_high_times_flag_bit2,
        status_code_high_times_flag_bit2_cume_dist,
        next_status_code_high_times_flag_bit2,
        prev_status_code_high,
        status_code_high_avg_6,
        status_code_high_times_flag_bit2_avg_2_prior,
        status_code_high_times_flag_bit2_avg_2_prior_rounded,
        SUM(status_code_high_times_flag_bit2_avg_2_prior) OVER (PARTITION BY attr_key ORDER BY status_code_high ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code_high_times_flag_bit2_avg_2_prior,
        LEAD(prev_status_code_high, 1) OVER (PARTITION BY attr_key ORDER BY status_code_high_times_flag_bit2_cume_dist ASC, id ASC, attr_key ASC) AS next_prev_status_code_high
    FROM base
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics_app_logs
    EXCEPT ALL
    SELECT id
    FROM app_logs
    WHERE host <> 'h2'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_app_logs s
    JOIN matched_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN next_status_code_high_times_flag_bit2 >= 69 THEN 'LARGE' ELSE 'SMALL' END AS next_status_code_high_times_flag_bit2_tier,
    CASE WHEN status_code_high_avg_6 >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_avg_6_tier,
    CASE WHEN status_code_high >= 2 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_tier,
    COUNT(*) FILTER (WHERE attr_key = 'version') AS version_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(status_code_high_times_flag_bit2_cume_dist) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS status_code_high_times_flag_bit2_cume_dist_per_region
FROM verified
GROUP BY ROLLUP (CASE WHEN next_status_code_high_times_flag_bit2 >= 69 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_code_high_avg_6 >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN status_code_high >= 2 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY version_rows DESC NULLS LAST;
