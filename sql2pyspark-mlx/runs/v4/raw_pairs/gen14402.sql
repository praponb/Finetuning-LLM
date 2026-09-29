SELECT
    *,
    SUM(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
    AVG(balance) OVER w_running AS customer_id_avg_balance,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS event_seq
FROM accounts
WINDOW w_running AS (PARTITION BY customer_id);
