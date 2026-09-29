SELECT id, status, balance
FROM (
    SELECT
        id,
        status,
        balance,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS rnk
    FROM accounts
) AS t
WHERE rnk <= 2
ORDER BY status
