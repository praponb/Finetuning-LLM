SELECT
    id,
    balance,
    customer_id,
    DATE_ADD(opened_date, 18) AS opened_date_plus_18d
FROM accounts
WHERE customer_id IS NULL
ORDER BY opened_date_plus_18d DESC, 2 DESC;
