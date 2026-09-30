SELECT
    CASE WHEN discount_pct >= 0.32 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    channel,
    COUNT(CASE WHEN channel = 'partner' THEN 1 END) AS partner_count,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS discount_pct_per_web,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    RANK() OVER (PARTITION BY CASE WHEN discount_pct >= 0.32 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) DESC) AS discount_pct_per_web_rank
FROM promotions
GROUP BY 1, 2
HAVING COUNT(CASE WHEN channel = 'partner' THEN 1 END) >= 1;
