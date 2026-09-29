WITH flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessionized AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY amount, id) AS rn
    FROM sessionized
    QUALIFY rn = 1
),
with_dims AS (
    SELECT
        a.*,
        b.amount AS order_amount,
        b.shipping_fee,
        b.status
    FROM latest a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
enriched AS (
    SELECT e.*, b.order_date
    FROM with_dims e
    INNER JOIN orders b
        ON e.order_id = b.id
),
scored AS (
    SELECT
        *,
        COUNT(burst_id) OVER (PARTITION BY order_id, status ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_burst_id,
        AVG(burst_id) OVER (PARTITION BY order_id, status ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_burst_id,
        SUM(burst_id) OVER (PARTITION BY order_id, status ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_burst_id,
        MAX(burst_id) OVER (PARTITION BY order_id, status ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_burst_id,
        SUM(order_amount) OVER (PARTITION BY method, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_order_amount
    FROM enriched
    WHERE paid_at < '2024-02-10'
)
SELECT *
FROM (
    SELECT
        method,
        order_id,
        status,
        running_max_burst_id
    FROM scored
) src
PIVOT (
    MAX(running_max_burst_id) AS value_sum,
    COUNT(1) AS cnt
    FOR status IN ('SHIPPED', 'CANCELLED', 'PENDING')
)
