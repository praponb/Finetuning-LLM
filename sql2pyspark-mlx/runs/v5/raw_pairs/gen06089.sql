WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(burst_id) OVER (PARTITION BY browser ORDER BY CAST(started_at AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS burst_id_sum_30d,
        AVG(burst_id) OVER (ORDER BY burst_id ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS burst_id_avg_5,
        SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_new_group_flag,
        RANK() OVER (PARTITION BY device, country ORDER BY new_group_flag DESC) AS new_group_flag_rank,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY burst_id DESC) AS burst_id_dense_rank
    FROM islands
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
islands_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        new_group_flag,
        burst_id,
        cumulative_new_group_flag,
        burst_id_dense_rank,
        SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM flagged_2
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        browser,
        started_at,
        cumulative_new_group_flag,
        burst_id_dense_rank,
        FIRST_VALUE(browser, TRUE) OVER (ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        LEAD(new_group_flag, 1) OVER (PARTITION BY device, country ORDER BY burst_id_dense_rank ASC, id ASC) AS next_new_group_flag,
        NTILE(5) OVER (ORDER BY burst_id_2 DESC, id ASC) AS burst_id_2_bucket
    FROM islands_sessions
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        browser,
        cumulative_new_group_flag,
        burst_id_dense_rank,
        next_new_group_flag,
        LEAST(cumulative_new_group_flag, next_new_group_flag) AS least_cumulative_new_group_flag_next_new_group_flag
    FROM enriched
    WHERE started_at > TIMESTAMP '2023-05-05 20:00:00'
      AND HOUR(started_at) BETWEEN 7 AND 10
)
SELECT
    id,
    browser,
    cumulative_new_group_flag,
    CASE WHEN browser RLIKE '^(Edge|Firefox)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS browser_zone
FROM cleaned;
