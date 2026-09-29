WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city,
        COALESCE(rating, 0) AS rating_filled
    FROM suppliers
    WHERE city IN ('Berlin', 'Tokyo')
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.cost
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY country), 4) AS cost_share,
        NTILE(10) OVER (PARTITION BY country, city ORDER BY rating_filled, id) AS rating_filled_bucket,
        LEAD(rating_filled, 1) OVER (PARTITION BY country ORDER BY cost, id) AS next_rating_filled
    FROM with_latest
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        cost,
        rating_filled_bucket,
        MIN(cost) OVER (PARTITION BY country, city) AS country_min_cost,
        LEAD(cost, 2) OVER (PARTITION BY country ORDER BY cost_share, id) AS next_cost,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY cost_share NULLS LAST) AS cost_share_percent_rank
    FROM scored
    WHERE rating_filled < 38
),
scored_suppliers AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(rating_filled_bucket) OVER w_hist AS running_max_rating_filled_bucket,
        COUNT(rating_filled_bucket) OVER w_hist AS running_count_rating_filled_bucket,
        SUM(rating_filled_bucket) OVER w_hist AS running_sum_rating_filled_bucket,
        LAST_VALUE(rating_filled_bucket) IGNORE NULLS OVER (ORDER BY cost_share_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_filled_bucket
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY city ORDER BY country_min_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.brand <> 'Globex'
        )
)
-- Grouped metrics
SELECT
    city,
    SUM(CASE WHEN city = 'New York' THEN next_cost ELSE 0 END) AS new_york_next_cost,
    APPROX_PERCENTILE(country_min_cost, ARRAY(0.25, 0.5, 0.75)) AS country_min_cost_quartiles,
    AVG(running_sum_rating_filled_bucket) AS avg_running_sum_rating_filled_bucket,
    SUM(CASE WHEN city = 'Singapore' THEN cost_share_percent_rank ELSE 0 END) AS singapore_cost_share_percent_rank
FROM not_flagged
GROUP BY city
HAVING SUM(CASE WHEN city = 'Singapore' THEN cost_share_percent_rank ELSE 0 END) >= 0
ORDER BY city DESC
