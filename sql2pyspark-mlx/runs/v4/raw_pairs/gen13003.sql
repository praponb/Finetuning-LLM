WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE txn_time >= TO_DATE('2024-05-30')
      AND merchant IS NOT NULL
)
-- QUALIFY-style window filter
SELECT *
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY is_flagged_flag DESC, id) = 1
