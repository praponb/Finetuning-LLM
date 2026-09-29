WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(island_id) OVER (PARTITION BY country, browser ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_island_id,
        AVG(island_id) OVER (PARTITION BY country, browser ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_island_id,
        COUNT(island_id) OVER (PARTITION BY country, browser ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_island_id
    FROM sessions_sessions
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY running_count_island_id DESC NULLS FIRST, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.city,
        p.credit_limit
    FROM deduped s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(island_id) OVER (ORDER BY running_max_island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_island_id,
        SUM(island_id) OVER (PARTITION BY browser, city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_island_id_2,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY device, customer_id ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        SUM(running_count_island_id) OVER (PARTITION BY device ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_count_island_id,
        NTILE(5) OVER (PARTITION BY device, customer_id ORDER BY running_max_island_id DESC, id) AS running_max_island_id_bucket
    FROM joined
)
-- Grouped metrics
SELECT
    browser,
    COUNT(DISTINCT device) AS distinct_devices,
    MAX(running_avg_island_id) FILTER (WHERE browser = 'Firefox') AS firefox_max_running_avg_island_id,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min
FROM with_windows
GROUP BY browser
HAVING (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 > 0
ORDER BY distinct_devices DESC NULLS FIRST;
