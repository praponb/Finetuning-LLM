SELECT id, amount, PERCENT_RANK() OVER (ORDER BY amount DESC) AS amount_pct_rank
FROM payments
WHERE method = 'paypal'
ORDER BY 3 DESC;
