SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY account_type ORDER BY opened_date DESC) AS row_num
    FROM accounts
) AS t
WHERE row_num = 1
