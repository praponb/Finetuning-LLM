WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        CASE WHEN page IN ('/checkout', '/product') THEN 'Tier 1' ELSE 'Other' END AS page_group
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-12-28 17:00:00'
      AND page IS NOT NULL
      AND duration_sec BETWEEN 136 AND 430
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.device,
        p.country
    FROM cleaned a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY session_id, page ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        FIRST_VALUE(page, TRUE) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page
    FROM enriched
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM web_events
    WHERE duration_sec IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN duration_sec >= 449 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
        CASE WHEN duration_sec_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_bucket_tier,
        SUM(CASE WHEN device = 'mobile' THEN duration_sec ELSE 0 END) AS mobile_duration_sec,
        AVG(duration_sec_bucket) AS avg_duration_sec_bucket,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        COUNT(*) AS row_count,
        SIZE(COLLECT_SET(device)) AS num_devices,
        APPROX_PERCENTILE(duration_sec_bucket, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_bucket_quartiles
    FROM verified
    GROUP BY CASE WHEN duration_sec >= 449 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN duration_sec_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END
    HAVING num_devices > 1
)
SELECT
    duration_sec_tier,
    duration_sec_bucket_tier,
    avg_duration_sec_bucket,
    avg_duration_sec,
    row_count
FROM summary
ORDER BY avg_duration_sec DESC NULLS LAST, avg_duration_sec_bucket;
