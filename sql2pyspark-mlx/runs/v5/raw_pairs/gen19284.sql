WITH cleaned AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-06-04 06:00:00'
      AND amount BETWEEN 2500 AND 3300
),
joined AS (
    SELECT
        a.id,
        a.account_id,
        a.amount,
        a.txn_time,
        a.merchant,
        a.is_flagged_flag,
        p2.balance,
        p2.account_type,
        p2.opened_date
    FROM cleaned a
    JOIN accounts p2
        ON a.account_id = p2.id
),
enriched AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged_flag,
        account_type,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share,
        LEAD(is_flagged_flag, 1, 0) OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC) AS next_is_flagged_flag
    FROM joined
    WHERE is_flagged_flag BETWEEN 28 AND 78
),
deduped AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type, account_id ORDER BY txn_time DESC, id ASC) = 1
)
SELECT
    s.id,
    s.account_id,
    s.amount,
    s.txn_time,
    s.merchant,
    s.account_type,
    s.amount_share,
    (SELECT SUM(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS sum_transaction_amount
FROM deduped s;
