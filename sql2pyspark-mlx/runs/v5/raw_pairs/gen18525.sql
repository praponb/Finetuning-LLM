SELECT
    id,
    balance,
    opened_date,
    customer_id,
    MONTH(opened_date) AS opened_date_month,
    LENGTH(status) AS status_length
FROM accounts
WHERE balance <= 28000
ORDER BY status_length DESC;
