WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS FIRST, id) AS rn
        FROM transactions
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    account_id,
    amount,
    txn_time,
    merchant,
    is_flagged
FROM latest;
