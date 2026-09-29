SELECT *
FROM (
    SELECT account_id, SUM(CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END) AS starbucks_count
    FROM transactions
    GROUP BY account_id
) AS t
WHERE starbucks_count > 74
ORDER BY starbucks_count DESC, account_id DESC
LIMIT 20
