WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
scored AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        is_new_burst,
        MAX(is_new_burst) OVER (PARTITION BY method, status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_is_new_burst,
        AVG(is_new_burst) OVER (PARTITION BY method, status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_is_new_burst,
        COUNT(is_new_burst) OVER (PARTITION BY method, status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_is_new_burst,
        LAST_VALUE(is_new_burst) IGNORE NULLS OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_is_new_burst
    FROM sessionized
),
joined AS (
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.amount,
        a.paid_at,
        a.is_new_burst,
        a.running_avg_is_new_burst,
        d.channel,
        d.amount AS order_amount
    FROM scored a
    LEFT JOIN orders d
        ON a.order_id <=> d.id
    WHERE d.status = 'RETURNED'
),
deduped AS (
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC NULLS LAST, id) = 1
),
with_gaps_2 AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM deduped
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
)
SELECT
    id,
    paid_at,
    is_new_burst,
    new_group_flag,
    ROUND(order_amount / SUM(order_amount) OVER (PARTITION BY method), 4) order_amount_share,
    ROW_NUMBER() OVER (ORDER BY is_new_burst DESC, id) seq_no,
    NTILE(3) OVER (ORDER BY running_avg_is_new_burst, id) running_avg_is_new_burst_bucket,
    NTILE(5) OVER (PARTITION BY method ORDER BY order_amount, id) order_amount_bucket,
    SUM(is_new_burst) OVER (PARTITION BY method ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_new_burst
FROM islands;
