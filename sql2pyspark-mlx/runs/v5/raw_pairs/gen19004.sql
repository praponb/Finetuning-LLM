WITH with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
enriched AS (
    SELECT
        id,
        method,
        status,
        prev_paid_at,
        new_group_flag,
        LEAD(new_group_flag, 1) OVER (PARTITION BY method, order_id ORDER BY paid_at, id) AS next_new_group_flag,
        AVG(island_id) OVER (PARTITION BY order_id, method) AS order_id_avg_island_id
    FROM sessionized
),
with_windows AS (
    SELECT
        id,
        status,
        prev_paid_at,
        next_new_group_flag,
        order_id_avg_island_id,
        AVG(order_id_avg_island_id) OVER (PARTITION BY status ORDER BY next_new_group_flag, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS order_id_avg_island_id_avg_5,
        PERCENT_RANK() OVER (PARTITION BY method ORDER BY next_new_group_flag DESC NULLS LAST) AS next_new_group_flag_percent_rank,
        LEAD(next_new_group_flag, 1) OVER (PARTITION BY method, status ORDER BY next_new_group_flag, id) AS next_next_new_group_flag
    FROM enriched
    WHERE order_id_avg_island_id = 6
),
ranked AS (
    SELECT
        id,
        status,
        prev_paid_at,
        order_id_avg_island_id_avg_5,
        next_new_group_flag_percent_rank,
        next_next_new_group_flag
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_id_avg_island_id_avg_5 DESC NULLS LAST, id) = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY next_new_group_flag_percent_rank DESC, id) AS rank_in_group
    FROM ranked
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    status,
    next_new_group_flag_percent_rank,
    next_next_new_group_flag,
    rank_in_group
FROM deduped
ORDER BY rank_in_group DESC NULLS FIRST;
