WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        NTILE(10) OVER w_grp AS amount_bucket,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM payments
    WINDOW w_grp AS (PARTITION BY order_id, method ORDER BY amount ASC, id ASC), w_ordered AS (PARTITION BY method ORDER BY paid_at ASC, id ASC)
),
scored_payments AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_bucket,
        row_seq,
        NTILE(4) OVER (ORDER BY amount_bucket ASC, id ASC) AS amount_bucket_bucket,
        LAG(amount, 1) OVER (ORDER BY row_seq ASC, id ASC) AS prev_amount,
        LEAD(amount_bucket, 1) OVER (PARTITION BY method, status ORDER BY amount ASC, id ASC) AS next_amount_bucket
    FROM scored
),
scored_payments_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER w_ordered AS row_seq_bucket,
        COUNT(next_amount_bucket) OVER w_part AS status_count_next_amount_bucket,
        LAG(next_amount_bucket, 2) OVER (PARTITION BY method ORDER BY prev_amount ASC, id ASC) AS prev_next_amount_bucket,
        FIRST_VALUE(method, TRUE) OVER (PARTITION BY method ORDER BY prev_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        LEAD(next_amount_bucket, 2) OVER (PARTITION BY method ORDER BY amount_bucket ASC, id ASC) AS next_next_amount_bucket
    FROM scored_payments
    WINDOW w_ordered AS (PARTITION BY status ORDER BY row_seq ASC, id ASC), w_part AS (PARTITION BY status, method)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY amount_bucket_bucket DESC) AS amount_bucket_bucket_dense_rank,
        ROUND(row_seq_bucket / SUM(row_seq_bucket) OVER (PARTITION BY order_id), 4) AS row_seq_bucket_share
    FROM scored_payments_2
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY method ORDER BY amount_bucket DESC NULLS FIRST) AS amount_bucket_cume_dist,
        SUM(amount_bucket_bucket) OVER (ORDER BY prev_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_bucket_bucket,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY row_seq_bucket ASC NULLS FIRST) AS row_seq_bucket_percent_rank
    FROM with_windows
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.status,
    s.amount_bucket,
    s.amount_bucket_bucket,
    s.prev_amount,
    s.next_amount_bucket,
    (SELECT SUM(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS sum_shipment_cost
FROM enriched s
