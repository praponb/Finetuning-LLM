WITH valid AS (
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END AS is_debit,
        CONCAT(txn_type, ', ', merchant) AS txn_type_merchant,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2023-05-18 03:00:00')
      AND merchant IS NOT NULL
),
matched_keys AS (
    SELECT merchant
    FROM valid
    INTERSECT
    SELECT merchant
    FROM transactions
    WHERE account_id IN (6, 19, 27)
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.merchant = k.merchant
),
base AS (
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        is_debit,
        merchant_filled,
        LOWER(merchant) AS merchant_lower,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM kept_rows
    WHERE merchant IS NOT NULL
),
filtered AS (
    SELECT
        id,
        txn_type,
        merchant_lower,
        resolved_merchant,
        ROUND(amount, 1) AS amount_rounded,
        COALESCE(merchant, 'OTHER') AS merchant_filled_2
    FROM base
    WHERE is_debit <= 20
),
with_windows AS (
    SELECT
        id,
        txn_type,
        merchant_lower,
        resolved_merchant,
        amount_rounded,
        merchant_filled_2,
        SUM(amount_rounded) OVER (PARTITION BY txn_type) AS txn_type_sum_amount_rounded,
        MAX(amount_rounded) OVER (PARTITION BY txn_type) AS txn_type_max_amount_rounded,
        PERCENT_RANK() OVER (PARTITION BY txn_type ORDER BY amount_rounded DESC NULLS FIRST) AS amount_rounded_percent_rank
    FROM filtered
)
SELECT
    id,
    txn_type,
    amount_rounded,
    merchant_filled_2,
    txn_type_max_amount_rounded
FROM with_windows
ORDER BY amount_rounded DESC NULLS LAST
