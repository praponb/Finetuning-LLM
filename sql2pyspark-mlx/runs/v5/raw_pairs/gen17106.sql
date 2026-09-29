SELECT
    channel,
    CASE WHEN discount_pct >= 0.32 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    AVG(discount_pct) AS avg_discount_pct,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev
FROM promotions
GROUP BY channel, CASE WHEN discount_pct >= 0.32 THEN 'HIGH' ELSE 'LOW' END
HAVING discount_pct_stddev > 0
ORDER BY discount_pct_tier ASC;
