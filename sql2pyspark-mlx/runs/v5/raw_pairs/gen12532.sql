SELECT
    id,
    account_type,
    opened_date,
    balance,
    DATE_TRUNC('month', opened_date) AS opened_date_period,
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label
FROM accounts
WHERE customer_id IS NOT NULL AND customer_id IN (13, 34, 35, 38) AND customer_id IS NOT NULL
ORDER BY opened_date_label, account_type;
