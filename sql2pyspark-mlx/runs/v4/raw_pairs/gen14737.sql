WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(country) country_lower
    FROM sessions
    WHERE started_at > DATE '2023-06-13'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        country_lower,
        is_new_burst,
        burst_id,
        NTILE(3) OVER (PARTITION BY browser, country ORDER BY burst_id, id) burst_id_bucket,
        NTILE(10) OVER (PARTITION BY device ORDER BY burst_id, id) burst_id_bucket_2,
        SUM(burst_id) OVER (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) burst_id_sum_7d
    FROM sessions_sessions
)
SELECT
    id,
    customer_id,
    device,
    browser,
    country,
    is_new_burst,
    burst_id,
    burst_id_bucket,
    burst_id_bucket_2
FROM enriched
ORDER BY burst_id_bucket_2 DESC;
