WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_avg_6,
        AVG(quantity) OVER (PARTITION BY order_id) AS order_id_avg_quantity
    FROM order_items
    WHERE order_id = 37
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY product_id, order_id ORDER BY quantity) = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.quantity,
        d.order_date,
        d.discount,
        d.status
    FROM deduped s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
with_dims_order_items AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.order_date,
        s.discount,
        d.amount,
        d.channel
    FROM with_dims s
    INNER JOIN orders d
        ON s.order_id = d.id
),
enriched_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        order_date,
        discount,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        NTILE(4) OVER (PARTITION BY channel ORDER BY discount DESC, id) AS discount_bucket
    FROM with_dims_order_items
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN discount >= 0.41 THEN 'LARGE' ELSE 'SMALL' END AS discount_tier,
        DATE_TRUNC('WEEK', order_date) AS week_start,
        PERCENTILE_APPROX(discount_bucket, 0.95) AS discount_bucket_p95,
        AVG(discount) AS avg_discount
    FROM enriched_order_items
    GROUP BY 1, 2
    HAVING AVG(discount) > 0
)
SELECT
    discount_tier,
    week_start,
    discount_bucket_p95
FROM daily
ORDER BY week_start DESC, discount_bucket_p95;
