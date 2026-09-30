SELECT a.status, COUNT(t.id) transaction_count, ROUND(STDDEV(t.amount), 2) stddev_amount
FROM accounts a
LEFT OUTER JOIN transactions t ON t.account_id = a.id
GROUP BY a.status
ORDER BY stddev_amount ASC, status ASC
LIMIT 5;
