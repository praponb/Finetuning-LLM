WITH cleaned AS (
    SELECT *, TRIM(account_type) AS clean_account_type
    FROM accounts
    WHERE opened_date > DATE '2023-05-27'
      AND status IS NOT NULL
),
metrics AS (
    SELECT
        *,
        LAG(balance) OVER w_hist AS prev_balance,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY status ORDER BY opened_date, id)
),
with_last AS (
    SELECT
        s.*,
        top1.transaction_merchant,
        top1.transaction_txn_type
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_accounts AS (
    SELECT
        s.id,
        s.customer_id,
        s.prev_balance,
        s.event_seq,
        s.transaction_merchant,
        top1.amount,
        top1.transaction_merchant_2
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.merchant AS transaction_merchant_2
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    SELECT
        s.id,
        s.customer_id,
        s.prev_balance,
        s.event_seq,
        x.merchant
    FROM with_last_accounts s
    JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    customer_id,
    COUNT_IF(merchant = '7-Eleven') AS 7_eleven_events,
    COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count,
    APPROX_PERCENTILE(prev_balance, ARRAY(0.25, 0.5, 0.75)) AS prev_balance_quartiles
FROM latest_match
GROUP BY CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, customer_id
ORDER BY 7_eleven_count NULLS LAST;
