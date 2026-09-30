SELECT
    *,
    STDDEV_SAMP(balance) OVER (PARTITION BY status) AS status_stddev_samp_balance,
    LAST_VALUE(status) IGNORE NULLS OVER (PARTITION BY account_type, status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
    SUM(balance) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM accounts
