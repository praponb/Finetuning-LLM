WITH with_dims AS (
    -- Attach suppliers attributes
    SELECT a.*, s.supplier_name
    FROM products a
    LEFT JOIN suppliers s
        ON a.supplier_id = s.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(launch_date, 43) AS launch_date_minus_43d
    FROM with_dims
    WHERE launch_date >= TO_DATE('2023-02-23')
      AND supplier_name NOT LIKE 'P%'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY category ORDER BY launch_date DESC) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date < s.launch_date
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(cost) OVER w_ordered AS running_count_cost,
        MAX(cost) OVER w_ordered AS running_max_cost,
        MIN(cost) OVER w_ordered AS running_min_cost
    FROM eligible
    WHERE supplier_name IS NOT NULL
    WINDOW w_ordered AS (PARTITION BY category, supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.cost,
        s.tags,
        s.launch_date_minus_43d,
        s.row_num,
        s.running_count_cost,
        s.running_max_cost,
        s.running_min_cost,
        top1.sale_date,
        top1.store_sale_revenue
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_products AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.launch_date_minus_43d,
        s.running_count_cost,
        s.running_max_cost,
        s.running_min_cost,
        s.store_sale_revenue,
        top1.order_item_unit_price,
        top1.order_item_quantity
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    running_max_cost,
    store_sale_revenue,
    order_item_unit_price,
    order_item_quantity
FROM with_latest_products
ORDER BY id
LIMIT 20;
