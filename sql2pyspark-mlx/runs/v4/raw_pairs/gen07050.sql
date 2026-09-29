SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS rn
    FROM accounts
) AS t
WHERE rn <= 3
