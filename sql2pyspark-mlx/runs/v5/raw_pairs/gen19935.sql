WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        order_date,
        channel,
        amount,
        shipping_fee,
        SUM(discount) OVER w_part AS running_sum_discount,
        MIN(discount) OVER w_part AS running_min_discount,
        AVG(discount) OVER w_part AS running_avg_discount,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM orders
    WINDOW w_part AS (PARTITION BY store_id, customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY store_id ORDER BY discount, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date < s.order_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        running_min_discount,
        AVG(running_min_discount) OVER (PARTITION BY channel ORDER BY event_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS running_min_discount_avg_5,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM not_flagged
    WHERE running_avg_discount IS NOT NULL
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.unit_price
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        running_min_discount,
        running_min_discount_avg_5,
        unit_price,
        LAG(running_min_discount) OVER (ORDER BY running_min_discount, id) AS prev_running_min_discount,
        LAG(running_min_discount_avg_5) OVER (ORDER BY running_min_discount, id) AS prev_running_min_discount_avg_5,
        ROW_NUMBER() OVER (ORDER BY running_min_discount, id) AS event_seq
    FROM with_latest
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_running_min_discount >= 0.45 THEN 'LARGE' ELSE 'SMALL' END AS prev_running_min_discount_tier,
    CASE WHEN running_min_discount >= 0.37 THEN 'LARGE' ELSE 'SMALL' END AS running_min_discount_tier,
    PERCENTILE_APPROX(event_seq, 0.5) AS event_seq_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(unit_price, ARRAY(0.25, 0.5, 0.75)) AS unit_price_quartiles,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN prev_running_min_discount >= 0.45 THEN 'LARGE' ELSE 'SMALL' END ORDER BY PERCENTILE_APPROX(event_seq, 0.5) DESC) AS event_seq_p50_dense_rank
FROM metrics
GROUP BY CASE WHEN prev_running_min_discount >= 0.45 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_min_discount >= 0.37 THEN 'LARGE' ELSE 'SMALL' END
HAVING distinct_ids > 1
ORDER BY event_seq_p50 NULLS LAST
