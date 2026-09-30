SELECT
    channel,
    promo_code,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    SUM(CASE WHEN promo_code = 'BUNDLE' THEN discount_pct ELSE 0 END) AS bundle_discount_pct,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev
FROM promotions
GROUP BY CUBE (channel, promo_code)
ORDER BY bundle_discount_pct
