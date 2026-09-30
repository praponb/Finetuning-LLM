WITH joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.discount,
        p.shipping_fee
    FROM shipments a
    JOIN orders p
        ON a.order_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        shipped_date,
        delivered_date,
        discount,
        shipping_fee,
        COUNT(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_cost,
        MAX(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_cost,
        MIN(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_cost
    FROM joined
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        MIN(shipping_fee) OVER w_running AS running_min_shipping_fee,
        AVG(shipping_fee) OVER w_running AS running_avg_shipping_fee,
        COUNT(shipping_fee) OVER w_running AS running_count_shipping_fee,
        SUM(shipping_fee) OVER w_running AS running_sum_shipping_fee
    FROM with_windows
    WINDOW w_running AS (ORDER BY last7_max_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(running_avg_shipping_fee) OVER (ORDER BY running_avg_shipping_fee, id) AS next_running_avg_shipping_fee,
        LEAD(running_sum_shipping_fee) OVER (ORDER BY running_avg_shipping_fee, id) AS next_running_sum_shipping_fee,
        ROW_NUMBER() OVER (ORDER BY running_avg_shipping_fee, id) AS seq_no,
        LAST_VALUE(running_sum_shipping_fee) IGNORE NULLS OVER (ORDER BY running_count_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_sum_shipping_fee
    FROM metrics
    WHERE running_sum_shipping_fee < 70
),
report AS (
    SELECT
        id,
        shipped_date,
        running_min_shipping_fee,
        running_avg_shipping_fee,
        running_count_shipping_fee,
        running_sum_shipping_fee,
        next_running_avg_shipping_fee,
        next_running_sum_shipping_fee,
        seq_no
    FROM enriched
)
SELECT
    id,
    shipped_date,
    running_min_shipping_fee,
    running_avg_shipping_fee,
    running_sum_shipping_fee,
    next_running_avg_shipping_fee,
    seq_no
FROM report
ORDER BY shipped_date;
