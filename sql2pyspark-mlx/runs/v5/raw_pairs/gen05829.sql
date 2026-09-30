WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        paid_at,
        status,
        CONCAT(method, '/', status) AS method_status,
        DATE_TRUNC('YEAR', paid_at) AS paid_at_period
    FROM payments
    WHERE amount BETWEEN 490 AND 1260
      AND method IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at_period DESC NULLS LAST, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        paid_at,
        method_status,
        paid_at_period,
        NTILE(5) OVER (PARTITION BY status ORDER BY amount DESC, id) AS amount_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share
    FROM deduped
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        paid_at,
        method_status,
        amount_bucket,
        amount_share,
        AVG(amount) OVER (ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        MAX(amount) OVER (ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        COUNT(amount) OVER (ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount
    FROM enriched
    WHERE paid_at_period >= '2023-11-01'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        running_avg_amount * amount_bucket AS running_avg_amount_times_amount_bucket,
        COALESCE(running_count_amount, 0) AS running_count_amount_filled
    FROM with_windows
    WHERE amount_share >= 0.57
)
-- Running and moving aggregates
SELECT
    id,
    amount_bucket,
    amount_share,
    running_avg_amount_times_amount_bucket,
    RANK() OVER (ORDER BY amount_bucket DESC NULLS LAST) AS amount_bucket_rank,
    NTILE(5) OVER (ORDER BY amount_share, id) AS amount_share_bucket,
    LEAD(running_count_amount_filled, 1) OVER (ORDER BY paid_at, id) AS next_running_count_amount_filled
FROM cleaned;
