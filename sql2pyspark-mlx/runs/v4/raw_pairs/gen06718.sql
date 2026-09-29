SELECT
    id,
    customer_id,
    balance,
    opened_date,
    FIRST_VALUE(opened_date) OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS first_opened_date
FROM accounts;
