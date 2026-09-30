WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(merchant) AS merchant_length,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM transactions
    WHERE txn_time > DATE '2023-06-05'
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        a.balance,
        a.status
    FROM cleaned e
    LEFT JOIN accounts a
        ON e.account_id = a.id
    WHERE a.account_type = 'LOAN'
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.amount,
        a.txn_time,
        a.merchant_length,
        d.balance AS account_balance
    FROM enriched a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY account_balance NULLS FIRST, id) AS dedup_rank
    FROM with_dims
    QUALIFY dedup_rank = 1
)
-- Join a dimension and aggregate
SELECT
    p.status,
    COUNT(*) AS num_rows,
    ROUND(SUM(a.merchant_length), 2) AS total_merchant_length,
    GROUPING_ID() AS level_id
FROM ranked a
INNER JOIN accounts p ON a.account_id = p.id
GROUP BY ROLLUP (p.status);
