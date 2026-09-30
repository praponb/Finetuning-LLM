WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        TRIM(method) AS clean_method
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-06-24 22:00:00'
      AND order_id IN (1, 7, 8, 34)
)
-- Keep one row per key (latest first)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC NULLS LAST, id) AS rn
    FROM filtered
) t
WHERE rn = 1
