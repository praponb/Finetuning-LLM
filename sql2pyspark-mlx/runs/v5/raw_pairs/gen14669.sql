WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(page) AS page_length,
        SUBSTRING(page, 1, 3) AS page_prefix
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-07-09 18:00:00'
      AND page IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(duration_sec) OVER (PARTITION BY page, event_type) AS page_min_duration_sec,
        LAG(page_length, 1) OVER (ORDER BY duration_sec ASC, id ASC) AS prev_page_length,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY page_length DESC) AS page_length_dense_rank
    FROM cleaned
    WHERE session_id IN (10, 25)
),
metrics_web_events AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        page_length,
        page_prefix,
        prev_page_length,
        page_length_dense_rank,
        COUNT(prev_page_length) OVER (PARTITION BY event_type) AS event_type_count_prev_page_length,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY page_length DESC, id ASC) AS event_seq
    FROM metrics
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, page
    FROM metrics_web_events
    INTERSECT ALL
    SELECT id, page
    FROM web_events
    WHERE page = '/product'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_web_events s
    INNER JOIN kept_keys k ON s.id = k.id AND s.page = k.page
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        d.device,
        d.country
    FROM kept_rows a
    INNER JOIN sessions d
        ON a.session_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        prev_page_length,
        device,
        country,
        COUNT(event_seq) OVER (ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS event_seq_count_7d,
        RANK() OVER (ORDER BY page_length_dense_rank DESC) AS page_length_dense_rank_rank
    FROM with_dims
)
-- Multi-level aggregation
SELECT
    device,
    page,
    ROUND(AVG(event_seq_count_7d), 2) AS avg_event_seq_count_7d,
    PERCENTILE_APPROX(event_seq_count_7d, ARRAY(0.25, 0.5, 0.75)) AS event_seq_count_7d_quartiles,
    PERCENTILE_APPROX(page_length_dense_rank_rank, ARRAY(0.25, 0.5, 0.75)) AS page_length_dense_rank_rank_quartiles,
    COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count
FROM enriched
GROUP BY GROUPING SETS ((device, page), (device), ());
