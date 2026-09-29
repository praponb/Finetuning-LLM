WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        LEAD(amount, 1) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC) AS next_amount,
        DENSE_RANK() OVER (PARTITION BY txn_type, account_id ORDER BY amount ASC NULLS FIRST) AS amount_dense_rank,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount ASC, id ASC) AS row_seq
    FROM transactions
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    CASE WHEN last_known_amount >= 4000 THEN 'LARGE' ELSE 'SMALL' END AS last_known_amount_tier,
    ROUND(SUM(next_amount) / NULLIF(COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END), 0), 2) AS next_amount_per_starbucks,
    APPROX_PERCENTILE(first_known_amount, 0.9) AS first_known_amount_p90,
    GROUPING_ID() AS gid
FROM scored
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', txn_time), CASE WHEN last_known_amount >= 4000 THEN 'LARGE' ELSE 'SMALL' END)
