WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        amount,
        discount,
        shipping_fee,
        LEAST(discount, amount) AS least_discount_amount
    FROM orders
    WHERE store_id = 8
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.product_id IS NULL
        )
),
kept_orders AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < s.order_date
        )
),
scored AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) AS shipping_fee_rank,
        COUNT(shipping_fee) OVER (PARTITION BY customer_id, store_id) AS customer_id_count_shipping_fee,
        SUM(discount) OVER (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount,
        LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS prev_amount,
        AVG(shipping_fee) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_2_prior
    FROM kept_orders
)
SELECT
    DATE_TRUNC('YEAR', order_date) AS year_start,
    CASE WHEN shipping_fee >= 31 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_tier,
    CORR(shipping_fee, amount) AS corr_shipping_fee_amount,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    ROUND(AVG(customer_id_count_shipping_fee), 2) AS avg_customer_id_count_shipping_fee,
    ROUND(AVG(prev_amount), 2) AS avg_prev_amount,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored
GROUP BY DATE_TRUNC('YEAR', order_date), CASE WHEN shipping_fee >= 31 THEN 'HIGH' ELSE 'LOW' END;
