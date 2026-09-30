SELECT
    promo_code,
    DATE_TRUNC('QUARTER', starts_on) AS quarter_start,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT(*) FILTER (WHERE channel = 'store') AS store_rows,
    COUNT_IF(promo_code = 'BUNDLE') AS bundle_events
FROM promotions
GROUP BY 1, 2
ORDER BY bundle_events DESC NULLS LAST;
