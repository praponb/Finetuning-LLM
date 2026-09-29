WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS FIRST, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT txn_type
    FROM latest
    EXCEPT ALL
    SELECT txn_type
    FROM transactions
    WHERE account_id IN (4, 6)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.txn_type = k.txn_type
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(amount, 0) amount_filled,
        COALESCE(merchant, 'UNKNOWN') resolved_merchant
    FROM verified
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM '7-Eleven'
),
filtered_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount_filled / amount, 2) amount_filled_per_amount,
        ROUND(amount_filled / amount, 2) amount_filled_per_amount_2
    FROM filtered
    WHERE merchant = 'Shell'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CAST(amount_filled AS INT) amount_filled_int
    FROM filtered_transactions
    WHERE txn_time > DATE '2023-10-06'
      AND merchant IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    d.account_type,
    e.txn_type,
    COUNT(*) AS cnt,
    AVG(e.amount_filled_int) AS avg_amount_filled_int,
    MAX(e.amount_filled) AS max_amount_filled
FROM cleaned e
INNER JOIN accounts d ON e.account_id = d.id
GROUP BY GROUPING SETS ((d.account_type, e.txn_type), (d.account_type))
ORDER BY max_amount_filled NULLS FIRST;
