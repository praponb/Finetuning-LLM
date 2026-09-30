SELECT
    DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
    channel,
    PERCENTILE_APPROX(discount_pct, 0.95) AS discount_pct_p95,
    SUM(discount_pct) AS total_discount_pct,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    SUM(discount_pct) FILTER (WHERE promo_code = 'VIP') AS vip_total_discount_pct,
    SUM(CASE WHEN channel = 'partner' THEN discount_pct ELSE 0 END) AS partner_discount_pct
FROM promotions
GROUP BY DATE_TRUNC('QUARTER', ends_on), channel;
