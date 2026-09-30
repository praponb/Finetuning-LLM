WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        DATE_FORMAT(event_time, 'MMM yyyy') AS event_time_label
    FROM web_events
    WHERE event_time >= DATE '2023-04-11'
      AND page IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, event_type
    FROM valid
    EXCEPT ALL
    SELECT id, event_type
    FROM web_events
    WHERE session_id IN (12, 25, 38)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN kept_keys k ON s.id = k.id AND s.event_type = k.event_type
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.device,
        p.browser,
        p.country
    FROM kept_rows src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        page,
        event_time_label,
        browser,
        country,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_90d,
        PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_percent_rank
    FROM joined
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(duration_sec_percent_rank) OVER (PARTITION BY page) AS page_stddev_samp_duration_sec_percent_rank,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec_avg_90d NULLS FIRST) AS duration_sec_avg_90d_dense_rank
    FROM enriched
),
metrics_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY browser, page ORDER BY duration_sec_avg_90d_dense_rank NULLS FIRST) AS duration_sec_avg_90d_dense_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY duration_sec_percent_rank DESC NULLS LAST) AS duration_sec_percent_rank_cume_dist,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY page_stddev_samp_duration_sec_percent_rank DESC NULLS FIRST) AS page_stddev_samp_duration_sec_percent_rank_dense_rank
    FROM metrics
    WHERE page IN ('/product', '/cart')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT session_id
    FROM metrics_web_events
    INTERSECT ALL
    SELECT session_id
    FROM web_events
    WHERE duration_sec = 153
),
kept_rows_web_events AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_web_events s
    JOIN remaining_keys k ON s.session_id = k.session_id
)
-- Running and moving aggregates
SELECT
    session_id,
    event_time_label,
    browser,
    country,
    duration_sec_avg_90d,
    duration_sec_percent_rank,
    page_stddev_samp_duration_sec_percent_rank,
    duration_sec_avg_90d_dense_rank_percent_rank,
    PERCENT_RANK() OVER (PARTITION BY page ORDER BY duration_sec_avg_90d_dense_rank DESC) AS duration_sec_avg_90d_dense_rank_percent_rank_2,
    ROUND(page_stddev_samp_duration_sec_percent_rank_dense_rank / SUM(page_stddev_samp_duration_sec_percent_rank_dense_rank) OVER (PARTITION BY page, session_id), 4) AS page_stddev_samp_duration_sec_percent_rank_dense_rank_share,
    DENSE_RANK() OVER w_recent AS duration_sec_percent_rank_cume_dist_dense_rank
FROM kept_rows_web_events
WINDOW w_recent AS (PARTITION BY page ORDER BY duration_sec_percent_rank_cume_dist DESC NULLS FIRST)
