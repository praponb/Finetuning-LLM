WITH valid AS (
    SELECT *, DATE_TRUNC('YEAR', order_date) AS order_date_period
    FROM orders
    WHERE customer_id IS NOT NULL
),
flagged AS (
    SELECT *, CASE WHEN LAG(order_date_period) OVER (PARTITION BY channel ORDER BY order_date_period, id) IS NULL
                 OR order_date_period > LAG(order_date_period) OVER (PARTITION BY channel ORDER BY order_date_period, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessions_orders AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY order_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY discount) AS discount_percent_rank,
        COUNT(is_new_session) OVER (PARTITION BY customer_id ORDER BY CAST(order_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS is_new_session_count_90d,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM sessions_orders
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC NULLS LAST, id) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
)
SELECT
    s.id,
    s.store_id,
    s.order_date,
    s.channel,
    s.amount,
    s.order_date_period,
    s.is_new_session,
    s.burst_id,
    s.discount_percent_rank,
    (SELECT MIN(t2.shipping_fee) FROM orders t2 WHERE t2.store_id = s.store_id) AS min_order_shipping_fee
FROM ranked s
ORDER BY order_date DESC NULLS FIRST
