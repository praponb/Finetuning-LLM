SELECT
    CASE WHEN balance >= 58000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    customer_id,
    COUNT_IF(status = 'OPEN') AS open_events,
    STDDEV_SAMP(balance) AS balance_stddev,
    COUNT(*) FILTER (WHERE status = 'OPEN') AS open_rows,
    STDDEV_SAMP(balance) AS balance_stddev_2
FROM accounts
GROUP BY CASE WHEN balance >= 58000 THEN 'LARGE' ELSE 'SMALL' END, customer_id
ORDER BY balance_stddev NULLS LAST
