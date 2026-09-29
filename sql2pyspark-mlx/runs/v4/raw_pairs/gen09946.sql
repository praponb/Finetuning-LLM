WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS latest_rank
    FROM promotions
    QUALIFY latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    CASE WHEN discount_pct >= 0.34 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_tier,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    COUNT(DISTINCT channel) AS distinct_channels,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct_2,
    APPROX_PERCENTILE(discount_pct, 0.95) AS discount_pct_p95
FROM latest
GROUP BY DATE_TRUNC('MONTH', ends_on), CASE WHEN discount_pct >= 0.34 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(discount_pct), 2) < 0
ORDER BY avg_discount_pct DESC;
