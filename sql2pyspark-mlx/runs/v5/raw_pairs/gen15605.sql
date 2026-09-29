WITH ranked_accounts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS rnk
    FROM accounts
    WHERE opened_date >= '2023-03-28'
)
SELECT *
FROM ranked_accounts
WHERE rnk <= 5;
