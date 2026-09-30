WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        CONCAT(browser, ' - ', country) AS browser_country,
        TRIM(device) AS clean_device,
        COALESCE(browser, 'UNKNOWN') AS browser_clean
    FROM sessions
    WHERE started_at > DATE '2023-06-25'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY started_at DESC, id) AS event_seq,
        FIRST_VALUE(country) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM base
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM metrics
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        clean_device,
        seq_no,
        row_seq,
        event_seq,
        SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY seq_no DESC NULLS LAST, id) AS row_num
    FROM sessions_sessions
    QUALIFY row_num = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        seq_no,
        row_seq,
        session_no,
        row_num,
        SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN country = 'TH' THEN 1 END), 0), 2) AS seq_no_per_th,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SIZE(COLLECT_SET(device)) AS num_devices,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list
FROM islands
GROUP BY 1
ORDER BY year_start DESC
