WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit,
        CASE WHEN txn_type IN ('REFUND', 'CREDIT') THEN 'Group A' ELSE 'Other' END AS txn_type_group
    FROM transactions
    WHERE txn_time > TIMESTAMP '2024-04-15 20:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY is_credit DESC, id) AS row_seq,
        NTILE(5) OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS amount_bucket,
        SUM(is_credit) OVER (PARTITION BY txn_type, merchant ORDER BY is_credit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_credit
    FROM valid
),
matched_keys AS (
    -- Set intersection on keys
    SELECT merchant
    FROM enriched
    INTERSECT ALL
    SELECT merchant
    FROM transactions
    WHERE txn_time >= '2025-02-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.merchant = k.merchant
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            merchant,
            txn_type,
            is_credit
        FROM kept_rows
    ) src
    PIVOT (
        MAX(is_credit)
        FOR txn_type IN ('CREDIT', 'REFUND')
    )
)
SELECT
    merchant,
    REFUND,
    CASE WHEN merchant RLIKE '^(Shell|Lazada)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS merchant_zone
FROM wide
ORDER BY merchant_zone NULLS LAST
