SELECT *
FROM (
    SELECT
        store_id,
        channel,
        SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_count,
        ROUND(STDDEV(discount), 2) AS stddev_discount
    FROM orders
    GROUP BY store_id, channel
) AS agg
WHERE web_count >= 76;
