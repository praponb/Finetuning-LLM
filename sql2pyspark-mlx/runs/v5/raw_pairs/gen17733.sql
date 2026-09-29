WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged
    FROM (
        SELECT *, RANK() OVER (PARTITION BY merchant, account_id ORDER BY amount DESC NULLS LAST) AS rn
        FROM transactions
    ) t
    WHERE rn = 1
)
SELECT
    id,
    txn_type,
    merchant,
    is_flagged,
    CASE WHEN merchant RLIKE '^(Lazada|7-Eleven)$' THEN 'PRIORITY' WHEN merchant RLIKE '^(Grab|Amazon)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS merchant_zone
FROM ranked
ORDER BY merchant_zone DESC;
