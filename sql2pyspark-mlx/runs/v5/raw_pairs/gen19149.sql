WITH base AS (
    SELECT *, UPPER(txn_type) AS txn_type_upper
    FROM transactions
    WHERE txn_time > DATE '2023-06-27'
)
SELECT
    *,
    NTILE(3) OVER (PARTITION BY merchant, txn_type ORDER BY amount DESC, id) AS amount_bucket,
    SUM(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_sum_90d,
    FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    NTILE(10) OVER w_running AS amount_bucket_2,
    NTILE(5) OVER w_running AS amount_bucket_3
FROM base
WHERE account_id = 39
WINDOW w_running AS (ORDER BY amount, id);
