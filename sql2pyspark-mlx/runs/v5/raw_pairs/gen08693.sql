WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount) OVER w_part AS amount_sum_30d,
        LEAD(amount, 2) OVER w_running AS next_amount,
        NTILE(5) OVER (ORDER BY amount, id) AS amount_bucket,
        FIRST_VALUE(merchant) IGNORE NULLS OVER w_recent AS first_known_merchant,
        NTILE(3) OVER (PARTITION BY account_id ORDER BY amount, id) AS amount_bucket_2
    FROM transactions
    WINDOW w_part AS (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY txn_type ORDER BY txn_time, id), w_recent AS (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.merchant,
    s.next_amount,
    s.amount_bucket,
    s.first_known_merchant,
    CASE WHEN merchant RLIKE '^(Starbucks|7-Eleven)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS merchant_zone,
    (SELECT MAX(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS max_transaction_amount
FROM scored s;
