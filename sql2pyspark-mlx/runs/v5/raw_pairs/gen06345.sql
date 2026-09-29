WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, WEEKOFYEAR(paid_at) AS paid_at_weekofyear
    FROM payments
    WHERE order_id IN (12, 24, 30)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY status, method) AS status_max_amount,
        LAG(amount, 2) OVER (PARTITION BY method ORDER BY paid_at_weekofyear ASC, id ASC) AS prev_amount
    FROM cleaned
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        amount,
        status,
        status_max_amount,
        NTILE(4) OVER (PARTITION BY status, method ORDER BY prev_amount DESC, id ASC) AS prev_amount_bucket,
        LEAD(status_max_amount, 1) OVER w_grp AS next_status_max_amount,
        SUM(amount) OVER w_ordered AS status_sum_amount
    FROM enriched
    WINDOW w_grp AS (PARTITION BY status ORDER BY amount ASC, id ASC), w_ordered AS (PARTITION BY status)
),
enriched_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY status_max_amount DESC) AS status_max_amount_percent_rank,
        NTILE(4) OVER (ORDER BY status_max_amount ASC, id ASC) AS status_max_amount_bucket,
        RANK() OVER (ORDER BY next_status_max_amount DESC) AS next_status_max_amount_rank
    FROM scored
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        method,
        amount,
        status,
        status_max_amount,
        prev_amount_bucket,
        next_status_max_amount,
        status_sum_amount,
        status_max_amount_bucket
    FROM enriched_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY next_status_max_amount DESC, id ASC) = 1
)
-- Aggregate with conditional metrics
SELECT
    status,
    COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count,
    COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) AS bank_transfer_count_2
FROM ranked
GROUP BY status
HAVING COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END) > 1
ORDER BY status DESC NULLS LAST
