WITH Numbered AS (
    SELECT
        id,
        account_type,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS rnk
    FROM accounts
)
SELECT *
FROM Numbered
WHERE rnk <= 3
ORDER BY opened_date ASC;
