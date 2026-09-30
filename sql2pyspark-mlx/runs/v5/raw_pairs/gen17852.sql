WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        LOWER(merchant) merchant_lower,
        COALESCE(merchant, 'NONE') merchant_clean
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-10-16 01:00:00'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        account_id,
        merchant,
        is_flagged,
        merchant_lower
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY txn_time DESC, id) AS rank_in_group
        FROM base
    ) x
    WHERE rank_in_group = 1
),
joined AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        b.status,
        b.opened_date,
        b.account_type
    FROM ranked e
    LEFT JOIN accounts b
        ON e.account_id = b.id
    WHERE b.balance IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('/', account_type, status) account_type_status_key,
        COALESCE(account_type, 'UNKNOWN') resolved_account_type
    FROM joined
    WHERE merchant IS NOT NULL
      AND status IS DISTINCT FROM 'CLOSED'
)
SELECT
    id,
    merchant_lower,
    opened_date,
    account_type_status_key,
    resolved_account_type
FROM cleaned
ORDER BY opened_date DESC;
