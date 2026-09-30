WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        CASE WHEN discount_pct >= 0.34 THEN 'High' WHEN discount_pct >= 0.18 THEN 'Medium' ELSE 'Low' END AS discount_pct_tier,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM promotions
    WHERE channel IS NOT NULL
      AND channel IS DISTINCT FROM 'mobile'
)
-- Grouped metrics
SELECT
    CASE WHEN discount_pct >= 0.34 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    AVG(discount_pct) AS avg_discount_pct,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles_2,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles_3
FROM base
GROUP BY CASE WHEN discount_pct >= 0.34 THEN 'HIGH' ELSE 'LOW' END;
