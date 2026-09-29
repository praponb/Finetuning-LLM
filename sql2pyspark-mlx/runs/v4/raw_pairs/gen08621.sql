SELECT
    account_type,
    customer_id,
    MIN(opened_date) AS min_opened_date,
    ROUND(STDDEV(balance), 2) AS stddev_balance
FROM accounts
GROUP BY CUBE(account_type, customer_id);
