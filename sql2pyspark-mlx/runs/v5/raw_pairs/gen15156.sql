WITH prev_events AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
metrics AS (
    SELECT
        id,
        method,
        amount,
        island_id,
        AVG(island_id) OVER (PARTITION BY method, status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS island_id_avg_90d,
        SUM(amount) OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM sessionized
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount, id) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
metrics_payments AS (
    SELECT
        id,
        method,
        SUM(island_id) OVER w_grp AS running_sum_island_id,
        MIN(island_id) OVER w_grp AS running_min_island_id,
        MAX(island_id) OVER w_grp AS running_max_island_id,
        AVG(island_id) OVER w_grp AS running_avg_island_id,
        SUM(island_id) OVER w_grp_2 AS running_island_id
    FROM deduped
    WINDOW w_grp AS (PARTITION BY method ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY method ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    method,
    running_sum_island_id,
    running_avg_island_id
FROM metrics_payments
ORDER BY id;
