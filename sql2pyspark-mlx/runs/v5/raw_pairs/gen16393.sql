SELECT
    CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    promo_code,
    COUNT(*) AS cnt,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    AVG(discount_pct) AS avg_discount_pct,
    COUNT_IF(promo_code = 'BUNDLE') AS bundle_events,
    MIN(discount_pct) AS min_discount_pct,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
FROM promotions
GROUP BY GROUPING SETS ((CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END, promo_code), (CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING bundle_events > 1
ORDER BY distinct_promo_codes DESC
