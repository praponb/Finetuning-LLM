WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM transactions
    WHERE TO_DATE(txn_time) < '2023-11-20'
      AND merchant IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_label,
        resolved_merchant,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label_2,
        CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END AS is_refund
    FROM filtered
    WHERE txn_time > DATE '2023-04-29'
      AND txn_type = 'DEBIT'
),
matched_keys AS (
    -- Set difference on keys
    SELECT account_id
    FROM cleaned
    EXCEPT ALL
    SELECT account_id
    FROM transactions
    WHERE txn_type <> 'DEBIT'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.account_id = k.account_id
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT merchant, account_id
    FROM kept_rows
    INTERSECT ALL
    SELECT merchant, account_id
    FROM transactions
    WHERE amount >= 1600
),
kept_rows_transactions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN matched_keys_2 k ON s.merchant = k.merchant AND s.account_id = k.account_id
)
SELECT
    s.account_id,
    s.is_flagged,
    s.is_flagged_label,
    s.resolved_merchant,
    s.is_flagged_label_2,
    s.is_refund,
    (SELECT SUM(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS sum_transaction_amount
FROM kept_rows_transactions s
ORDER BY sum_transaction_amount DESC;
