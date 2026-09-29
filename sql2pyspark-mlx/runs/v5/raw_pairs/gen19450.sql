WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.cost > 570
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        rating,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rating_avg_5_prior,
        LEAD(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM kept
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND ARRAY_CONTAINS(x.tags, 'bestseller')
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        rating_avg_5_prior,
        rating_share,
        MAX(rating_share) OVER (ORDER BY rating_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_rating_share,
        SUM(rating_share) OVER (ORDER BY rating_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_rating_share,
        COUNT(rating_share) OVER (ORDER BY rating_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_rating_share,
        LAG(rating_share, 1) OVER (ORDER BY next_rating, id) AS prev_rating_share
    FROM eligible
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN rating_avg_5_prior >= 4 THEN 'HIGH' ELSE 'LOW' END AS rating_avg_5_prior_tier,
    CASE WHEN prev_rating_share >= 0.86 THEN 'HIGH' ELSE 'LOW' END AS prev_rating_share_tier,
    PERCENTILE_APPROX(running_max_rating_share, ARRAY(0.25, 0.5, 0.75)) AS running_max_rating_share_quartiles,
    ROUND(AVG(running_count_rating_share), 2) AS avg_running_count_rating_share,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(rating_share, ARRAY(0.25, 0.5, 0.75)) AS rating_share_quartiles,
    ROUND(AVG(rating_avg_5_prior), 2) AS avg_rating_avg_5_prior,
    APPROX_PERCENTILE(running_sum_rating_share, ARRAY(0.25, 0.5, 0.75)) AS running_sum_rating_share_quartiles
FROM scored
GROUP BY CASE WHEN rating_avg_5_prior >= 4 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN prev_rating_share >= 0.86 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_rating_avg_5_prior
