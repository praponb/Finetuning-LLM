WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        DATE_TRUNC('month', event_time) AS event_time_period
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-04-13 18:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        event_time_period,
        LAST_VALUE(page, TRUE) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page,
        SUM(duration_sec) OVER (PARTITION BY page, session_id ORDER BY CAST(event_time_period AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_sum_90d
    FROM valid
),
enriched_web_events AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        event_time_period,
        last_known_page,
        duration_sec_sum_90d,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec_sum_90d ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time_period DESC, id ASC) AS seq_no
    FROM enriched
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched_web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_sum_90d ASC, id ASC) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY duration_sec ASC NULLS FIRST) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
),
ranked_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time_period DESC, id ASC) AS rn
        FROM ranked
    ) x
    WHERE rn = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    MIN(event_time_period) AS first_event_time_period,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev
FROM ranked_web_events
GROUP BY 1
ORDER BY duration_sec_stddev DESC NULLS LAST
