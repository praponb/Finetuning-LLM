SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    RANK() OVER (PARTITION BY customer_id ORDER BY balance DESC NULLS FIRST) balance_rank,
    STDDEV_SAMP(balance) OVER (PARTITION BY account_type) account_type_stddev_samp_balance,
    SUM(balance) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_balance,
    CUME_DIST() OVER (PARTITION BY customer_id, account_type ORDER BY balance DESC NULLS LAST) balance_cume_dist
FROM accounts
