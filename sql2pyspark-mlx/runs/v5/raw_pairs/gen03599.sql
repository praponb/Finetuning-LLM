WITH cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        ROUND(quantity, 1) AS quantity_rounded
    FROM order_items
    WHERE order_id IN (18, 36)
),
scored AS (
    SELECT
        id,
        order_id,
        NTILE(5) OVER w_running AS quantity_rounded_bucket,
        SUM(quantity_rounded) OVER w_ordered AS running_quantity_rounded,
        SUM(unit_price) OVER w_grp AS running_unit_price
    FROM cleaned
    WHERE product_id = 2
    WINDOW w_running AS (PARTITION BY product_id ORDER BY quantity_rounded, id), w_ordered AS (PARTITION BY product_id, order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    SELECT
        a.id,
        a.order_id,
        a.quantity_rounded_bucket,
        b.shipping_fee,
        b.discount,
        b.channel
    FROM scored a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
joined AS (
    SELECT
        s.id,
        s.order_id,
        s.discount,
        o.shipping_fee AS order_shipping_fee,
        o.order_date
    FROM with_dims s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
with_dims_order_items AS (
    SELECT
        s.id,
        s.order_id,
        s.discount,
        s.order_date,
        p.shipping_fee,
        p.status,
        p.discount AS order_discount
    FROM joined s
    JOIN orders p
        ON s.order_id = p.id
),
summary AS (
    SELECT
        DATE_TRUNC('YEAR', order_date) AS year_start,
        DATE_TRUNC('MONTH', order_date) AS activity_month,
        APPROX_PERCENTILE(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles,
        SIZE(COLLECT_SET(status)) AS num_status
    FROM with_dims_order_items
    GROUP BY DATE_TRUNC('YEAR', order_date), DATE_TRUNC('MONTH', order_date)
    HAVING num_status >= 1
)
SELECT
    year_start,
    activity_month,
    num_status
FROM summary
ORDER BY num_status DESC
