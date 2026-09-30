SELECT
    CASE WHEN discount_pct >= 0.26 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    promo_code,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END), 0), 2) AS discount_pct_per_welcome5
FROM promotions
GROUP BY CASE WHEN discount_pct >= 0.26 THEN 'HIGH' ELSE 'LOW' END, promo_code
HAVING discount_pct_per_welcome5 > 0
ORDER BY discount_pct_per_welcome5 DESC NULLS LAST;
