SELECT
    id,
    customer_id,
    balance,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date, id) seq_no,
    LAST_VALUE(status, TRUE) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_status
FROM accounts
WHERE MONTH(opened_date) IN (6, 7, 10);
