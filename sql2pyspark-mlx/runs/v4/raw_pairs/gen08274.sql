WITH joined AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.device,
        p.browser
    FROM web_events a
    JOIN sessions p
        ON a.session_id = p.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec NULLS FIRST, id) AS rn
    FROM joined
    QUALIFY rn = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.browser AS session_browser,
        p.device AS session_device
    FROM latest src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY session_browser ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        CUME_DIST() OVER (PARTITION BY device ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY event_type, device ORDER BY dedup_rank DESC, id) AS seq_no,
        AVG(duration_sec) OVER (ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_avg_2,
        NTILE(10) OVER (ORDER BY duration_sec, id) AS duration_sec_bucket_2
    FROM with_dims
),
latest_web_events AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS FIRST, id) AS rank_in_group
        FROM with_windows
    ) x
    WHERE rank_in_group = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        duration_sec_bucket_2 * duration_sec_avg_2 AS duration_sec_bucket_2_times_duration_sec_avg_2,
        COALESCE(page, 'NONE') AS resolved_page
    FROM latest_web_events
    WHERE event_time > TIMESTAMP '2023-12-28 06:00:00'
      AND device IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        browser,
        page,
        duration_sec_bucket_2
    FROM base
) src
PIVOT (
    SUM(duration_sec_bucket_2) AS value_sum
    FOR page IN ('/product' AS product, '/checkout' AS checkout, '/cart' AS cart)
);
