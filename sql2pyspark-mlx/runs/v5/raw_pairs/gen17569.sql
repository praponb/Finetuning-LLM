SELECT
    d.channel,
    e.status AS payment_status,
    COUNT(*) AS rows_total,
    MIN(e.amount) AS min_amount,
    GROUPING_ID() AS level_id
FROM payments e
INNER JOIN orders d ON e.order_id = d.id
GROUP BY GROUPING SETS ((d.channel, e.status), (d.channel))
ORDER BY min_amount DESC NULLS LAST
