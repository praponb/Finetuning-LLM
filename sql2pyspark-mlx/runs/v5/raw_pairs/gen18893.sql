SELECT
    dim.status AS order_status,
    dim.channel,
    COUNT(*) AS rows_total,
    ROUND(AVG(s.amount), 2) AS avg_amount
FROM payments s
INNER JOIN orders dim ON s.order_id = dim.id
GROUP BY CUBE (dim.status, dim.channel)
ORDER BY rows_total DESC NULLS LAST
