SELECT *, ROUND(balance * 1.07, 2) AS adjusted_balance, COALESCE(status, 'Unknown') AS status_filled
FROM accounts
WHERE (opened_date <= '2023-03-01' AND balance <= 39000) OR balance IS NULL
ORDER BY customer_id
