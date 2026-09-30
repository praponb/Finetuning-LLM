SELECT
    id,
    customer_id,
    account_type,
    opened_date,
    status,
    LEAD(balance, 1) OVER w_hist AS next_balance,
    ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY balance DESC, id) AS seq_no,
    ROW_NUMBER() OVER w_running AS row_seq
FROM accounts
WINDOW w_hist AS (ORDER BY balance, id), w_running AS (PARTITION BY status, account_type ORDER BY balance DESC, id);
