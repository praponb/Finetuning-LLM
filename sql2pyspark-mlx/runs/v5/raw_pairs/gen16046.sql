SELECT
    DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
    MIN(discount_pct) AS min_discount_pct,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END) AS spring10_count
FROM promotions
GROUP BY 1
ORDER BY distinct_ids NULLS LAST
