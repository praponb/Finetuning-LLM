WITH enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.shipping_fee,
        d.status,
        d.order_date
    FROM order_items e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(quantity) OVER (PARTITION BY product_id) AS product_id_min_quantity,
        SUM(shipping_fee) OVER (PARTITION BY order_id) AS order_id_sum_shipping_fee,
        SUM(quantity) OVER w_ordered AS cumulative_quantity,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_order_items_2 AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.brand,
        p.price,
        p.launch_date
    FROM enriched_order_items a
    LEFT JOIN products p
        ON a.product_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    AVG(price) FILTER (WHERE status = 'COMPLETED') AS completed_avg_price,
    PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END), 0), 2) AS quantity_per_completed
FROM enriched_order_items_2
GROUP BY 1
ORDER BY quantity_per_completed DESC NULLS LAST
