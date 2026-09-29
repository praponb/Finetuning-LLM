WITH attributed AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.starts_on BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    channel,
    COUNT_IF(promotion_promo_code = 'SPRING10') AS spring10_events,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    ROUND(AVG(promo_id), 2) AS avg_promo_id,
    MIN(promo_id) AS min_promo_id,
    PERCENTILE_APPROX(promotion_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS promotion_discount_pct_quartiles,
    APPROX_PERCENTILE(promotion_discount_pct, 0.5) AS promotion_discount_pct_p50
FROM attributed
GROUP BY ROLLUP (channel)
ORDER BY spring10_events DESC NULLS FIRST;
