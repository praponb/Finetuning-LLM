WITH enriched AS (
    SELECT
        *,
        RANK() OVER (ORDER BY quantity NULLS LAST) AS quantity_rank,
        COUNT(unit_price) OVER (PARTITION BY order_id) AS order_id_count_unit_price,
        LAG(unit_price, 1) OVER w_hist AS prev_unit_price,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM order_items
    WHERE unit_price BETWEEN 240 AND 350
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY unit_price, id)
),
enriched_order_items AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_rank,
        order_id_count_unit_price,
        last_known_quantity,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY order_id_count_unit_price DESC, id) AS order_id_count_unit_price_bucket,
        SUM(unit_price) OVER (ORDER BY quantity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
        NTILE(10) OVER w_running AS quantity_bucket
    FROM enriched
    WINDOW w_running AS (PARTITION BY order_id ORDER BY quantity, id)
),
remaining_keys AS (
    SELECT id, product_id
    FROM enriched_order_items
    INTERSECT ALL
    SELECT id, product_id
    FROM order_items
    WHERE product_id IN (17, 33)
),
reconciled AS (
    SELECT s.*
    FROM enriched_order_items s
    JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
enriched_order_items_2 AS (
    SELECT
        src.order_id,
        src.product_id,
        src.quantity,
        src.order_id_count_unit_price,
        src.last_known_quantity,
        src.order_id_count_unit_price_bucket,
        src.cumulative_unit_price,
        src.quantity_bucket,
        p.cost,
        p.category,
        p.product_name
    FROM reconciled src
    JOIN products p
        ON src.product_id = p.id
),
joined AS (
    SELECT
        a.order_id,
        a.product_id,
        a.quantity,
        a.category,
        a.product_name,
        p.stock_qty
    FROM enriched_order_items_2 a
    LEFT JOIN products p
        ON a.product_id = p.id
),
enriched_order_items_3 AS (
    SELECT e.*, b.channel
    FROM joined e
    JOIN orders b
        ON e.order_id = b.id
),
with_dims AS (
    SELECT src.*, d.category AS product_category
    FROM enriched_order_items_3 src
    LEFT JOIN products d
        ON src.product_id = d.id
)
SELECT
    order_id,
    quantity,
    product_name,
    stock_qty,
    product_category,
    AVG(stock_qty) OVER (PARTITION BY product_category) AS product_category_avg_stock_qty,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY channel), 4) AS quantity_share,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share_2
FROM with_dims
