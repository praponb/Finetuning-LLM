SELECT CASE WHEN channel IN ('mobile', 'partner') THEN 'core' ELSE 'other' END AS channel_group, MAX(amount) - MIN(amount) AS amount_range, MAX(discount) AS max_discount
FROM orders
GROUP BY channel_group
