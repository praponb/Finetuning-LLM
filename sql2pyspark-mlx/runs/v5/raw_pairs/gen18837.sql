WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        status,
        CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END AS is_bank_transfer,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE method IS DISTINCT FROM 'paypal'
      AND method IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_hist AS is_bank_transfer_bucket,
        LEAD(is_bank_transfer, 1) OVER (PARTITION BY method ORDER BY is_bank_transfer ASC, id ASC) AS next_is_bank_transfer
    FROM filtered
    WINDOW w_hist AS (ORDER BY is_bank_transfer ASC, id ASC)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY next_is_bank_transfer DESC, id ASC) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        status,
        resolved_method,
        next_is_bank_transfer,
        AVG(is_bank_transfer_bucket) OVER (PARTITION BY method ORDER BY is_bank_transfer ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_bank_transfer_bucket_avg_5_prior,
        PERCENT_RANK() OVER (ORDER BY is_bank_transfer_bucket DESC NULLS LAST) AS is_bank_transfer_bucket_percent_rank,
        SUM(is_bank_transfer) OVER (PARTITION BY order_id, status ORDER BY is_bank_transfer_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_bank_transfer,
        FIRST_VALUE(is_bank_transfer, TRUE) OVER (PARTITION BY method, status ORDER BY next_is_bank_transfer ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_bank_transfer
    FROM ranked
),
summary AS (
    -- Grouped metrics
    SELECT
        order_id,
        APPROX_PERCENTILE(first_known_is_bank_transfer, ARRAY(0.25, 0.5, 0.75)) AS first_known_is_bank_transfer_quartiles,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count,
        PERCENTILE_APPROX(next_is_bank_transfer, ARRAY(0.25, 0.5, 0.75)) AS next_is_bank_transfer_quartiles,
        COUNT(*) FILTER (WHERE method = 'paypal') AS paypal_rows
    FROM with_windows
    GROUP BY order_id
)
SELECT
    order_id,
    first_known_is_bank_transfer_quartiles,
    refunded_count,
    next_is_bank_transfer_quartiles,
    paypal_rows
FROM summary
