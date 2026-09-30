SELECT
    channel,
    ROUND(AVG(discount), 2) AS avg_discount,
    SUM(amount) FILTER (WHERE channel = 'mobile') AS mobile_total_amount
FROM orders
GROUP BY ROLLUP (channel)
ORDER BY channel DESC NULLS LAST;
