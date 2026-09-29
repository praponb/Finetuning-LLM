WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS tier_value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN currency IN ('EUR', 'USD') THEN 'Core' ELSE 'Other' END AS currency_group
    FROM flattened
    WHERE currency IN ('THB', 'EUR')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        currency_group,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_dense_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value) AS tier_value_dense_rank_2,
        SUM(tier_value) OVER (PARTITION BY currency) AS currency_sum_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_dense_rank_3
    FROM cleaned
)
-- Grouped metrics
SELECT
    CASE WHEN tier_value >= 82 THEN 'HIGH' ELSE 'LOW' END AS tier_value_tier,
    PERCENTILE_APPROX(tier_value_dense_rank, 0.95) AS tier_value_dense_rank_p95,
    APPROX_PERCENTILE(currency_sum_tier_value, 0.95) AS currency_sum_tier_value_p95,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    APPROX_PERCENTILE(tier_value, ARRAY(0.25, 0.5, 0.75)) AS tier_value_quartiles,
    ROUND(AVG(tier_value_dense_rank), 2) AS avg_tier_value_dense_rank,
    ROUND(AVG(tier_value_dense_rank_3), 2) AS avg_tier_value_dense_rank_3
FROM enriched
GROUP BY tier_value_tier
ORDER BY avg_tier_value_dense_rank_3 DESC;
