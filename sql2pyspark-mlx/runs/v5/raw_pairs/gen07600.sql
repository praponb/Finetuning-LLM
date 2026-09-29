WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
kept_keys AS (
    SELECT id, plan
    FROM exploded
    EXCEPT DISTINCT
    SELECT id, plan
    FROM price_tiers
    WHERE effective_from > '2024-11-01'
),
verified AS (
    SELECT s.*
    FROM exploded s
    JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
enriched AS (
    SELECT
        effective_from,
        currency,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val NULLS FIRST) AS val_cume_dist
    FROM verified
    WHERE plan = 'basic'
),
metrics AS (
    SELECT
        effective_from,
        currency,
        RANK() OVER (ORDER BY val_cume_dist DESC) AS val_cume_dist_rank,
        PERCENT_RANK() OVER w_part AS val_cume_dist_percent_rank,
        DENSE_RANK() OVER w_part AS val_cume_dist_dense_rank
    FROM enriched
    WINDOW w_part AS (PARTITION BY currency ORDER BY val_cume_dist)
)
SELECT
    CASE WHEN val_cume_dist_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS val_cume_dist_dense_rank_tier,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    ROUND(AVG(val_cume_dist_dense_rank), 2) AS avg_val_cume_dist_dense_rank,
    COUNT_IF(currency = 'USD') AS usd_events,
    PERCENTILE_APPROX(val_cume_dist_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS val_cume_dist_percent_rank_quartiles
FROM metrics
GROUP BY CASE WHEN val_cume_dist_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', effective_from), DATE_TRUNC('YEAR', effective_from)
HAVING ROUND(AVG(val_cume_dist_dense_rank), 2) >= 0
ORDER BY avg_val_cume_dist_dense_rank DESC NULLS FIRST;
