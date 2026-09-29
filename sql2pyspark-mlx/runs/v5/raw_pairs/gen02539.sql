WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END AS is_failed
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-09-16 22:00:00'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        is_failed,
        STDDEV_SAMP(is_failed) OVER (PARTITION BY order_id) AS order_id_stddev_samp_is_failed,
        LEAD(is_failed, 1, 0) OVER w_hist AS next_is_failed,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY is_failed DESC, id) AS is_failed_bucket
    FROM filtered
    WHERE is_failed <> 50
    WINDOW w_hist AS (PARTITION BY method ORDER BY paid_at, id)
),
enriched_payments AS (
    -- Running and moving aggregates
    SELECT
        id,
        paid_at,
        status,
        is_failed,
        order_id_stddev_samp_is_failed,
        is_failed_bucket,
        DENSE_RANK() OVER (PARTITION BY method, status ORDER BY order_id_stddev_samp_is_failed DESC NULLS LAST) AS order_id_stddev_samp_is_failed_dense_rank,
        LEAD(is_failed_bucket, 1) OVER (ORDER BY is_failed, id) AS next_is_failed_bucket,
        AVG(is_failed) OVER (PARTITION BY method ORDER BY order_id_stddev_samp_is_failed, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS is_failed_avg_3_prior,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY is_failed_bucket DESC NULLS FIRST) AS is_failed_bucket_dense_rank
    FROM enriched
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id_stddev_samp_is_failed,
        is_failed_bucket,
        order_id_stddev_samp_is_failed_dense_rank,
        next_is_failed_bucket,
        is_failed_avg_3_prior,
        is_failed_bucket_dense_rank
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY is_failed_bucket DESC NULLS FIRST) AS latest_rank
        FROM enriched_payments
    ) x
    WHERE latest_rank = 1
)
SELECT
    id,
    order_id_stddev_samp_is_failed_dense_rank,
    next_is_failed_bucket,
    is_failed_avg_3_prior
FROM ranked
