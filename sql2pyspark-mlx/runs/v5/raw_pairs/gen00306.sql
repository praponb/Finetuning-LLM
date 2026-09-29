WITH not_flagged AS (
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at >= s.order_date
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.order_id IS NOT NULL
        )
),
metrics AS (
    SELECT
        id,
        customer_id,
        order_date,
        channel,
        discount,
        AVG(discount) OVER w_part last7_avg_discount,
        COUNT(discount) OVER w_part last7_count_discount,
        SUM(discount) OVER w_part last7_sum_discount,
        MIN(discount) OVER w_part last7_min_discount
    FROM eligible
    WINDOW w_part AS (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
with_last AS (
    SELECT
        s.*,
        latest.shipped_date,
        latest.delivered_date
    FROM metrics s
    JOIN LATERAL (
            SELECT c.shipped_date, c.delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
cleaned AS (
    SELECT
        *,
        DATEDIFF(order_date, delivered_date) days_delivered_date_to_order_date,
        last7_avg_discount + last7_count_discount last7_avg_discount_plus_last7_count_discount
    FROM with_last
    WHERE channel IS DISTINCT FROM 'store'
      AND shipped_date < '2023-12-01'
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.shipped_date
        )
)
SELECT
    d.segment,
    src.channel,
    COUNT(*) AS cnt,
    MIN(src.last7_sum_discount) AS min_last7_sum_discount
FROM kept src
LEFT JOIN customers d ON src.customer_id = d.id
GROUP BY d.segment, src.channel
ORDER BY channel DESC NULLS LAST;
