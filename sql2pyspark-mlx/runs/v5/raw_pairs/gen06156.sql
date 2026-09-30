SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN balance >= 84000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    STDDEV_SAMP(balance) AS balance_stddev,
    COUNT_IF(status = 'CLOSED') AS closed_events,
    COUNT(*) AS num_rows,
    STDDEV_SAMP(balance) AS balance_stddev_2
FROM accounts
GROUP BY DATE_TRUNC('QUARTER', opened_date), CASE WHEN balance >= 84000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING num_rows >= 1
ORDER BY num_rows DESC NULLS FIRST;
