WITH scored AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        MIN(amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        MAX(amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        COUNT(amount) OVER (PARTITION BY merchant, account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share
    FROM transactions
    WHERE account_id IN (15, 24, 32, 35)
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.merchant,
    s.is_flagged,
    s.running_min_amount,
    s.running_count_amount,
    s.amount_share,
    CASE WHEN merchant RLIKE '^(Amazon|Lazada)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS merchant_zone,
    (SELECT MIN(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS min_transaction_amount
FROM scored s
ORDER BY merchant_zone DESC NULLS LAST
