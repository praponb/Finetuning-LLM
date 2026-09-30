WITH valid AS (
    SELECT
        *,
        LOWER(txn_type) AS txn_type_lower,
        CAST(amount AS INT) AS amount_int,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-11-22 00:00:00'
      AND merchant IN ('Grab', 'Shell')
),
cleaned AS (
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        txn_type_lower,
        LEAST(amount_int, amount) AS least_amount_int_amount,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM valid
    WHERE txn_time >= TIMESTAMP '2023-12-08 22:00:00'
),
valid_transactions AS (
    SELECT *, CAST(least_amount_int_amount AS INT) AS least_amount_int_amount_int
    FROM cleaned
    WHERE merchant IS NOT NULL
      AND merchant <> 'Amazon'
),
joined AS (
    SELECT s.*, a.account_type
    FROM valid_transactions s
    LEFT JOIN accounts a
        ON s.account_id = a.id
)
SELECT
    id,
    account_id,
    txn_type,
    merchant,
    txn_type_lower,
    least_amount_int_amount,
    least_amount_int_amount_int,
    account_type
FROM joined
ORDER BY least_amount_int_amount_int DESC;
