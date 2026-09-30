SELECT
    status,
    CASE WHEN discount >= 0.38 THEN 'LARGE' ELSE 'SMALL' END AS discount_tier,
    SUM(CASE WHEN channel = 'partner' THEN shipping_fee ELSE 0 END) AS partner_shipping_fee,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END), 0), 2) AS amount_per_cancelled,
    STDDEV_SAMP(amount) AS amount_stddev,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM orders
GROUP BY status, CASE WHEN discount >= 0.38 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY distinct_ids
