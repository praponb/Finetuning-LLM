WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        s.event_time,
        s.duration_sec,
        p2.browser
    FROM web_events s
    JOIN sessions p2
        ON s.session_id = p2.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id) AS next_duration_sec,
        LAG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS seq_no
    FROM enriched
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
scored_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(seq_no) OVER (PARTITION BY browser ORDER BY prev_duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS seq_no_avg_6_prior,
        SUM(next_duration_sec) OVER (PARTITION BY browser ORDER BY prev_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS event_seq
    FROM sessionized
    WHERE duration_sec BETWEEN 157 AND 448
),
kept_keys AS (
    -- Set difference on keys
    SELECT session_id, page
    FROM scored_web_events
    EXCEPT
    SELECT session_id, page
    FROM web_events
    WHERE event_type = 'search'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_web_events s
    JOIN kept_keys k ON s.session_id = k.session_id AND s.page = k.page
)
-- Grouped metrics
SELECT
    CASE WHEN running_next_duration_sec >= 2202 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_next_duration_sec_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    AVG(seq_no) FILTER (WHERE event_type = 'purchase') AS purchase_avg_seq_no,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count
FROM kept_rows
GROUP BY 1, 2
HAVING home_count > 1
ORDER BY purchase_avg_seq_no DESC;
