WITH filtered AS (
    SELECT *, FLOOR(amount) AS amount_whole
    FROM payments
    WHERE status = 'REFUNDED'
),
enriched AS (
    SELECT
        *,
        AVG(amount) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
        AVG(amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_avg_7d,
        AVG(amount_whole) OVER (PARTITION BY status ORDER BY amount_whole ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_whole_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY amount ASC, id ASC) AS row_seq,
        SUM(amount_whole) OVER (PARTITION BY order_id, status ORDER BY amount_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_whole
    FROM filtered
),
gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessions_payments AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
metrics AS (
    SELECT
        *,
        LAG(burst_id, 1, 0) OVER (ORDER BY amount ASC, id ASC) AS prev_burst_id,
        RANK() OVER (ORDER BY amount ASC) AS amount_rank,
        LAST_VALUE(is_new_burst) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY amount_avg_6_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_new_burst,
        NTILE(5) OVER (PARTITION BY status ORDER BY amount ASC, id ASC) AS amount_bucket
    FROM sessions_payments
),
scored AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_whole,
        amount_avg_6_prior,
        amount_avg_7d,
        amount_whole_avg_6_prior,
        row_seq,
        running_amount_whole,
        is_new_burst,
        burst_id,
        prev_burst_id,
        amount_rank,
        last_known_is_new_burst,
        amount_bucket,
        STDDEV_SAMP(amount_avg_6_prior) OVER (PARTITION BY method) AS method_stddev_samp_amount_avg_6_prior,
        NTILE(3) OVER (ORDER BY row_seq ASC, id ASC) AS row_seq_bucket,
        AVG(running_amount_whole) OVER (PARTITION BY status ORDER BY amount_bucket ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_amount_whole_avg_3_prior,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY prev_burst_id ASC, id ASC) AS prev_burst_id_bucket,
        MIN(amount_avg_6_prior) OVER (PARTITION BY method, order_id) AS method_min_amount_avg_6_prior
    FROM metrics
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.status,
    s.amount_whole,
    s.amount_avg_6_prior,
    s.amount_avg_7d,
    s.amount_whole_avg_6_prior,
    s.row_seq,
    (SELECT MAX(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS max_shipment_cost
FROM scored s
ORDER BY amount_avg_6_prior DESC
