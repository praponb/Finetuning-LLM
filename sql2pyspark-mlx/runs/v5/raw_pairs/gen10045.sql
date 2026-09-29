WITH filtered AS (
    SELECT
        *,
        ROUND(amount, 1) AS amount_rounded,
        CONCAT_WS('/', merchant, txn_type) AS merchant_txn_type_key
    FROM transactions
    WHERE amount > 2500
)
SELECT
    id,
    amount,
    merchant,
    is_flagged,
    amount_rounded,
    merchant_txn_type_key,
    COUNT(amount) OVER (PARTITION BY txn_type) AS txn_type_count_amount,
    LAG(amount_rounded, 1) OVER (PARTITION BY merchant ORDER BY amount, id) AS prev_amount_rounded,
    SUM(amount) OVER w_ordered AS running_amount,
    LEAD(amount, 1) OVER (PARTITION BY merchant, account_id ORDER BY amount_rounded, id) AS next_amount
FROM filtered
WINDOW w_ordered AS (PARTITION BY merchant, txn_type ORDER BY amount_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
