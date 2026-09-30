WITH enriched AS (
    -- Attach accounts attributes
    SELECT src.*, d.account_type
    FROM transactions src
    INNER JOIN accounts d
        ON src.account_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        account_type,
        CASE WHEN txn_type IN ('FEE', 'CREDIT') THEN 'Priority' ELSE 'Other' END txn_type_group
    FROM enriched
    WHERE txn_time >= DATE '2023-07-21'
      AND account_type IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        account_type,
        CAST(amount AS INT) amount_int,
        CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END is_fee
    FROM valid
    WHERE txn_time > TIMESTAMP '2023-10-31 12:00:00'
      AND account_type IS NOT NULL
      AND amount <= 1900
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', txn_time) AS year_start,
        txn_type,
        MIN(txn_time) AS started_at,
        COUNT(*) AS num_rows,
        GROUPING_ID() AS gid
    FROM base
    GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', txn_time), txn_type), (DATE_TRUNC('YEAR', txn_time)))
)
SELECT
    year_start,
    txn_type,
    started_at,
    num_rows,
    gid,
    CASE WHEN txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS txn_type_zone
FROM summary
