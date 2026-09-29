WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        LAST_VALUE(browser, TRUE) OVER w_grp AS last_known_browser,
        LAST_VALUE(browser, TRUE) OVER w_grp AS last_known_browser_2,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS row_seq
    FROM sessions
    WINDOW w_grp AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.web_event_event_type
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY web_event_event_type ORDER BY row_seq DESC, id) AS seq_no,
        SUM(row_seq) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        NTILE(3) OVER (ORDER BY row_seq DESC, id) AS row_seq_bucket,
        AVG(row_seq) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS row_seq_avg_2,
        STDDEV_SAMP(row_seq) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_row_seq
    FROM latest_match
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, c.credit_limit
    FROM with_windows e
    INNER JOIN customers c
        ON e.customer_id = c.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM joined
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessionized
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps_2
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY seq_no DESC, id) AS dedup_rank
        FROM sessions_sessions
    ) x
    WHERE dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    island_id,
    PERCENTILE_APPROX(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(device)) AS num_devices
FROM deduped
GROUP BY island_id
HAVING COUNT(*) > 1
ORDER BY island_id DESC;
