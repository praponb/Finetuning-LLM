WITH filtered AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        ROUND(discount / amount, 2) AS discount_per_amount
    FROM orders
    WHERE store_id IS NOT NULL
      AND shipping_fee < 16
),
with_windows AS (
    SELECT
        *,
        MIN(shipping_fee) OVER (PARTITION BY status, customer_id) AS status_min_shipping_fee,
        CORR(shipping_fee, amount) OVER (PARTITION BY channel) AS corr_shipping_fee_amount
    FROM filtered
    WHERE shipping_fee > 37
),
with_last AS (
    SELECT
        s.*,
        x.payment_method,
        x.payment_amount
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.method AS payment_method, c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows_orders AS (
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        shipping_fee,
        discount_per_amount,
        status_min_shipping_fee,
        corr_shipping_fee_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY payment_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        SUM(corr_shipping_fee_amount) OVER (ORDER BY status_min_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_corr_shipping_fee_amount,
        FIRST_VALUE(discount_per_amount) IGNORE NULLS OVER (PARTITION BY channel ORDER BY discount_per_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_per_amount
    FROM with_last
),
totals AS (
    SELECT
        store_id,
        DATE_TRUNC('YEAR', order_date) AS year_start,
        COUNT(DISTINCT status) AS distinct_status,
        SIZE(COLLECT_SET(status)) AS num_status,
        COUNT(*) AS num_rows
    FROM with_windows_orders
    GROUP BY store_id, DATE_TRUNC('YEAR', order_date)
)
SELECT
    s.store_id,
    s.year_start,
    s.distinct_status,
    s.num_rows,
    (SELECT MIN(x.revenue) FROM store_sales x WHERE x.store_id = s.store_id) AS min_store_sale_revenue
FROM totals s
ORDER BY year_start NULLS LAST
