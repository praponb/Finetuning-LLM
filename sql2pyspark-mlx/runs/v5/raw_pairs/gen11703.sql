WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        ROUND(amount * 0.9, 2) AS amount_scaled
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_type IN ('REFUND', 'DEBIT', 'FEE')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT txn_type, id
    FROM cleaned
    INTERSECT
    SELECT txn_type, id
    FROM transactions
    WHERE txn_time >= '2024-06-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.txn_type = k.txn_type AND s.id = k.id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        d.balance,
        d.opened_date
    FROM kept_rows src
    LEFT JOIN accounts d
        ON src.account_id = d.id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.account_id,
        src.txn_type,
        src.merchant,
        src.is_flagged,
        b.balance AS account_balance,
        b.account_type,
        b.opened_date AS account_opened_date
    FROM joined src
    LEFT JOIN accounts b
        ON src.account_id = b.id
    WHERE b.customer_id IS NOT NULL
)
SELECT
    account_id,
    txn_type,
    merchant,
    is_flagged,
    account_balance,
    account_type,
    account_opened_date,
    CASE WHEN txn_type RLIKE '^(FEE|DEBIT)$' THEN 'CORE' WHEN txn_type RLIKE '^(CREDIT|REFUND)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS txn_type_zone
FROM with_dims
ORDER BY account_type DESC NULLS FIRST;
