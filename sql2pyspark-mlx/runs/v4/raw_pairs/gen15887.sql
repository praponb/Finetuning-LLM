SELECT CASE WHEN channel IN ('mobile', 'partner') THEN 'tier_1' ELSE 'other' END AS channel_group, channel, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM orders
GROUP BY CASE WHEN channel IN ('mobile', 'partner') THEN 'tier_1' ELSE 'other' END, channel
HAVING ROUND(STDDEV(amount), 2) >= 52
ORDER BY stddev_amount DESC, channel_group DESC;
