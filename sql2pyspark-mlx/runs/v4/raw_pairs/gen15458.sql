WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_type ORDER BY opened_date DESC NULLS LAST) AS dedup_rank
        FROM accounts
    ) x
    WHERE dedup_rank = 1
)
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    SUM(CASE WHEN status = 'OPEN' THEN balance ELSE 0 END) AS open_balance,
    STDDEV_SAMP(balance) AS balance_stddev,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
FROM deduped
GROUP BY DATE_TRUNC('QUARTER', opened_date)
ORDER BY open_balance NULLS LAST
