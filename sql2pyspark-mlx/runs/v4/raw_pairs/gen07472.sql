WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN amount >= 3500 THEN 'High' WHEN amount >= 900 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        LOWER(txn_type) AS txn_type_lower
    FROM transactions
    WHERE merchant IS NOT NULL
      AND account_id = 12
),
valid_transactions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(merchant) AS merchant_lower,
        CAST(txn_time AS DATE) AS txn_time_day
    FROM valid
    WHERE txn_time >= TIMESTAMP '2024-04-13 16:00:00'
      AND merchant IS NOT NULL
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        d.opened_date,
        d.account_type
    FROM valid_transactions s
    LEFT JOIN accounts d
        ON s.account_id = d.id
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.merchant,
    s.is_flagged,
    s.amount_tier,
    s.txn_time_day,
    s.opened_date,
    CASE WHEN txn_type RLIKE '^(FEE|DEBIT)$' THEN 'GROUP_B' WHEN txn_type RLIKE '^(CREDIT|REFUND)$' THEN 'EXTENDED' ELSE 'CORE' END AS txn_type_zone,
    (SELECT SUM(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS sum_transaction_amount
FROM with_dims s
ORDER BY txn_type_zone;
