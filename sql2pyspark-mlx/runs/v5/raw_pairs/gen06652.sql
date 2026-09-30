SELECT account_id, COUNT(*) cnt, MIN(amount) min_amount
FROM transactions
GROUP BY account_id
HAVING COUNT(*) < 3
ORDER BY account_id;
