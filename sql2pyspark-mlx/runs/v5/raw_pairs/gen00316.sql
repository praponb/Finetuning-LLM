SELECT discount, channel, UPPER(channel) AS channel_upper
FROM orders
WHERE amount > 710 AND amount < 700 AND customer_id IS NOT NULL
ORDER BY 3 ASC
