SELECT account_type, ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
WHERE balance IS NOT NULL AND account_type NOT IN ('LOAN', 'SAVINGS', 'CHECKING')
GROUP BY account_type
HAVING ROUND(AVG(balance), 2) > 77
ORDER BY account_type;
