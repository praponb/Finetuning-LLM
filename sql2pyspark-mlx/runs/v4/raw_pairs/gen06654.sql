WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(email, 'N/A') AS email_filled,
        COALESCE(segment, 'DIRECT') AS segment_filled
    FROM customers
    WHERE signup_date > DATE '2023-10-24'
      AND country = 'DE'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date BETWEEN '2024-02-01' AND '2024-06-03'
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment, country), 4) AS credit_limit_share,
        ROUND(age / SUM(age) OVER (PARTITION BY segment), 4) AS age_share,
        NTILE(4) OVER w_part AS age_bucket,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS credit_limit_dense_rank
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY country ORDER BY age DESC, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at >= s.signup_date
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_dense_rank_tier,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    ROUND(AVG(age), 2) AS avg_age,
    GROUPING_ID() AS grouping_id
FROM kept
GROUP BY ROLLUP (CASE WHEN credit_limit_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END);
