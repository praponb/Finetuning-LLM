WITH joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.credit_limit,
        d.name,
        d.country AS customer_country
    FROM sessions src
    JOIN customers d
        ON src.customer_id = d.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY browser ORDER BY credit_limit DESC, id) credit_limit_bucket,
        COUNT(credit_limit) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) credit_limit_count_7d,
        AVG(credit_limit) OVER (ORDER BY started_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) credit_limit_avg_2,
        LAG(credit_limit, 1) OVER (PARTITION BY device ORDER BY started_at, id) prev_credit_limit
    FROM kept
)
-- Grouped metrics
SELECT
    CASE WHEN credit_limit_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_bucket_tier,
    customer_country,
    MIN(started_at) AS first_seen,
    ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN customer_country = 'UK' THEN 1 END), 0), 2) AS credit_limit_per_uk,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(CASE WHEN customer_country = 'JP' THEN 1 END) AS jp_count,
    COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list
FROM metrics
GROUP BY CASE WHEN credit_limit_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END, customer_country
ORDER BY jp_count NULLS LAST;
