SELECT *
FROM (
    SELECT channel, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM orders
    GROUP BY channel
) AS agg
WHERE stddev_amount < 63
ORDER BY stddev_amount DESC
