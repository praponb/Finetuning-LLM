WITH with_dims AS (
    SELECT s.*, d.category
    FROM order_items s
    LEFT JOIN products d
        ON s.product_id = d.id
),
metrics AS (
    SELECT
        *,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id, order_id), 4) unit_price_share,
        MAX(unit_price) OVER (PARTITION BY order_id) order_id_max_unit_price,
        AVG(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) quantity_avg_3,
        PERCENT_RANK() OVER (ORDER BY quantity DESC) quantity_percent_rank
    FROM with_dims
),
with_dims_order_items AS (
    SELECT
        src.*,
        p.stock_qty,
        p.launch_date,
        p.price
    FROM metrics src
    JOIN products p
        ON src.product_id = p.id
),
joined AS (
    SELECT s.*, d.launch_date AS product_launch_date
    FROM with_dims_order_items s
    LEFT JOIN products d
        ON s.product_id = d.id
),
scored AS (
    SELECT
        *,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) unit_price_share_2,
        AVG(unit_price_share) OVER (PARTITION BY product_id ORDER BY order_id_max_unit_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) unit_price_share_avg_2_prior,
        ROUND(price / SUM(price) OVER (PARTITION BY order_id), 4) price_share,
        AVG(quantity_percent_rank) OVER (PARTITION BY order_id ORDER BY quantity_avg_3, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) quantity_percent_rank_avg_3,
        SUM(quantity_avg_3) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity_avg_3
    FROM joined
),
enriched AS (
    SELECT a.*, p.launch_date AS product_launch_date_2
    FROM scored a
    LEFT JOIN products p
        ON a.product_id = p.id
)
SELECT
    order_id,
    DATE_TRUNC('QUARTER', product_launch_date_2) AS quarter_start,
    COUNT_IF(category = 'Books') AS books_events,
    MAX(unit_price_share_avg_2_prior) FILTER (WHERE category = 'Grocery') AS grocery_max_unit_price_share_avg_2_prior,
    COUNT_IF(category = 'Sports') AS sports_events,
    COUNT(CASE WHEN category = 'Books' THEN 1 END) AS books_count,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM enriched
GROUP BY order_id, DATE_TRUNC('QUARTER', product_launch_date_2);
