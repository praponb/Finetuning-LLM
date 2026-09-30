WITH account_totals AS (
    SELECT account_id, COUNT(DISTINCT amount) AS unique_merchants, SUM(CASE WHEN merchant = 'Starbucks' THEN amount ELSE 0 END) AS starbucks_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    at.unique_merchants,
    at.starbucks_amount
FROM accounts a
LEFT JOIN account_totals at ON a.id = at.account_id
WHERE at.starbucks_amount > 54
