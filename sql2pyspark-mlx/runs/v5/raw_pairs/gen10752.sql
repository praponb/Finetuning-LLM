SELECT
    promo_code,
    PERCENTILE_APPROX(discount_pct, 0.5) AS discount_pct_p50,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    SUM(discount_pct) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_total_discount_pct
FROM promotions
GROUP BY 1
