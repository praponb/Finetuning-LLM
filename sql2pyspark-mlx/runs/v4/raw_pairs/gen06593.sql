WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        INITCAP(TRIM(txn_type)) AS txn_type_title,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM transactions
    WHERE txn_time > TIMESTAMP '2024-02-04 23:00:00'
      AND merchant IS NOT NULL
      AND txn_type = 'DEBIT'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC NULLS LAST, id) AS dedup_rank
        FROM cleaned
    ) t
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach accounts attributes
    SELECT a.*, p.balance
    FROM deduped a
    JOIN accounts p
        ON a.account_id = p.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        balance * amount AS balance_times_amount,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM joined
    WHERE txn_time >= DATE '2023-06-29'
      AND merchant IS NOT NULL
      AND is_flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN balance_times_amount >= 65 THEN 'LARGE' ELSE 'SMALL' END AS balance_times_amount_tier,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    ROUND(AVG(amount), 2) AS avg_amount,
    MIN(txn_time) AS first_seen
FROM filtered
GROUP BY 1;
