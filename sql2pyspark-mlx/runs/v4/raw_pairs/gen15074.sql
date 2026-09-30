WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id ASC) AS row_num
        FROM promotions
    ) t
    WHERE row_num = 1
)
SELECT
    CASE WHEN discount_pct >= 0.33 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    COUNT_IF(promo_code = 'SPRING10') AS spring10_events,
    MIN(discount_pct) AS min_discount_pct
FROM latest
GROUP BY discount_pct_tier
ORDER BY avg_discount_pct ASC;
