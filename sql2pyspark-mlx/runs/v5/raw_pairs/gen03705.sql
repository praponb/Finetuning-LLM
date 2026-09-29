WITH with_last AS (
    SELECT
        s.id,
        s.account_type,
        s.balance,
        s.opened_date,
        latest.transaction_merchant,
        latest.amount
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    account_type,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    COUNT(*) AS event_count,
    AVG(amount) AS avg_amount,
    COUNT(DISTINCT transaction_merchant) AS distinct_transaction_merchants,
    ROUND(AVG(amount), 2) AS avg_amount_2,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(amount, 0.95) AS amount_p95,
    GROUPING(account_type) AS gid
FROM with_last
GROUP BY GROUPING SETS ((account_type, DATE_TRUNC('QUARTER', opened_date)), (account_type), ());
