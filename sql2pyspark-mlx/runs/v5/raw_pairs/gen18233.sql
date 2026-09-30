SELECT
    id,
    customer_id,
    balance,
    status,
    LAST_VALUE(status) OVER (PARTITION BY customer_id ORDER BY balance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_status,
    PERCENT_RANK() OVER (PARTITION BY status ORDER BY balance DESC) AS balance_pct_rank
FROM accounts;
