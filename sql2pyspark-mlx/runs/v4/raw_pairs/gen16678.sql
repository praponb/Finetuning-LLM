WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        CONCAT(account_type, ' ', status) AS account_type_status,
        COALESCE(status, 'OTHER') AS status_clean
    FROM accounts
    WHERE status IS NOT NULL
)
-- Keep one row per key (latest first)
SELECT *
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date DESC, id) = 1;
