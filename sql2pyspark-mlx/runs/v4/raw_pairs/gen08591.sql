WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_2,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code, channel), 4) AS discount_pct_share_2
    FROM promotions
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', ends_on) AS year_start,
    DATE_TRUNC('QUARTER', starts_on) AS quarter_start,
    promo_code,
    PERCENTILE_APPROX(running_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS running_discount_pct_quartiles,
    ROUND(AVG(running_discount_pct), 2) AS avg_running_discount_pct,
    SUM(CASE WHEN promo_code = 'BUNDLE' THEN discount_pct ELSE 0 END) AS bundle_discount_pct,
    AVG(running_discount_pct_2) FILTER (WHERE channel = 'mobile') AS mobile_avg_running_discount_pct_2
FROM scored
GROUP BY 1, 2, 3
