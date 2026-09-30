WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-07-07'
      AND status IS NOT NULL
),
base_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        opened_date_label,
        status_filled,
        CONCAT(status, ', ', account_type) AS status_account_type,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM base
    WHERE status IS NOT NULL
      AND status IS DISTINCT FROM 'FROZEN'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.status_filled,
        c.country,
        c.segment
    FROM base_accounts s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.segment,
        p.city,
        p.age
    FROM enriched a
    JOIN customers p
        ON a.customer_id = p.id
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.txn_time
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant != '7-Eleven'
        )
)
SELECT
    id,
    segment,
    age,
    txn_time,
    CASE WHEN segment RLIKE '^(Corporate|SMB)$' THEN 'GROUP_B' WHEN segment RLIKE '^(Consumer)$' THEN 'EXTENDED' ELSE 'CORE' END AS segment_zone
FROM not_flagged
ORDER BY id DESC NULLS FIRST
