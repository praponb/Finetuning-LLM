SELECT a.account_type, a.status, MIN(t.amount) AS min_amount
FROM transactions t
JOIN accounts a ON t.account_id = a.id
WHERE t.is_flagged = FALSE
GROUP BY a.account_type, a.status
HAVING MIN(t.amount) > 1100
ORDER BY min_amount;
