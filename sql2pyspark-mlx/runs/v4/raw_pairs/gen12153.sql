WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(status) AS status_length
    FROM subscriptions
    WHERE start_date > DATE '2023-07-19'
      AND customer_id IS NOT NULL
      AND start_date BETWEEN '2024-12-01' AND '2025-06-01'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY status_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_6,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY status_length DESC, id) AS status_length_bucket,
        LEAD(monthly_fee, 1, 0) OVER (PARTITION BY plan, status ORDER BY monthly_fee, id) AS next_monthly_fee
    FROM cleaned
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC NULLS FIRST, id) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM ranked
    EXCEPT DISTINCT
    SELECT id, status
    FROM subscriptions
    WHERE plan IN ('basic', 'premium')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.id = k.id AND s.status = k.status
)
-- Window metrics over several different partitions
SELECT
    *,
    RANK() OVER (PARTITION BY plan ORDER BY status_length_bucket DESC NULLS FIRST) AS status_length_bucket_rank,
    SUM(monthly_fee) OVER (PARTITION BY status) AS status_sum_monthly_fee,
    RANK() OVER (ORDER BY next_monthly_fee NULLS FIRST) AS next_monthly_fee_rank
FROM reconciled
