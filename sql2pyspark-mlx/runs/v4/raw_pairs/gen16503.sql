WITH with_dims AS (
    -- Attach accounts attributes
    SELECT e.*, a.account_type
    FROM transactions e
    INNER JOIN accounts a
        ON e.account_id = a.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior,
        NTILE(4) OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS amount_bucket
    FROM with_dims
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, WEEKOFYEAR(txn_time) AS txn_time_weekofyear
    FROM scored
    WHERE txn_time > DATE '2023-08-06'
      AND merchant IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Window metrics over several different partitions
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    account_type,
    amount_avg_2_prior,
    amount_bucket,
    txn_time_weekofyear,
    is_new_burst,
    island_id,
    SUM(is_new_burst) OVER (ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_burst,
    NTILE(4) OVER (PARTITION BY account_type ORDER BY amount_bucket DESC, id) AS amount_bucket_bucket,
    LAST_VALUE(amount_bucket, TRUE) OVER (ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_bucket
FROM sessionized
