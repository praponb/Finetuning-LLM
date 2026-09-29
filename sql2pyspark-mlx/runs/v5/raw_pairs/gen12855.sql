WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount, 1, 0) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS next_amount,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant
    FROM transactions
)
SELECT
    id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    next_amount,
    first_known_merchant
FROM scored
