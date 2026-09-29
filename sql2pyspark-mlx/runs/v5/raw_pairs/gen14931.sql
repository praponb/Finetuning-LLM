SELECT *
FROM (
    SELECT channel, COUNT(DISTINCT amount) unique_statuss, ROUND(STDDEV(amount), 2) stddev_amount
    FROM orders
    WHERE status IN ('PENDING', 'RETURNED')
    GROUP BY channel
) s
WHERE unique_statuss < 4
