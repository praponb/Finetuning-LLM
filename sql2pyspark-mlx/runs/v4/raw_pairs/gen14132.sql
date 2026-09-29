SELECT account_type, COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count
FROM accounts
WHERE opened_date >= '2024-12-30' AND (account_type NOT IN ('SAVINGS', 'LOAN') OR customer_id = 13)
GROUP BY account_type
ORDER BY open_count DESC
