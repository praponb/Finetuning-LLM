SELECT *, SUBSTRING(channel, 1, 2) AS channel_prefix
FROM orders
WHERE discount > 0.14
ORDER BY amount DESC, discount DESC
