SELECT
    id,
    status,
    balance,
    customer_id,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS row_num,
    FIRST_VALUE(opened_date) OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS first_opened_date
FROM accounts;
