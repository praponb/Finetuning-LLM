WITH valid AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        LOWER(product_name) AS product_name_lower
    FROM products
    WHERE launch_date > DATE '2023-06-02'
),
with_dims AS (
    SELECT
        e.*,
        b.city,
        b.supplier_name
    FROM valid e
    LEFT JOIN suppliers b
        ON e.supplier_id = b.id
),
with_latest AS (
    SELECT
        s.*,
        x.order_item_unit_price,
        x.quantity
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        product_name_lower,
        city,
        supplier_name,
        order_item_unit_price,
        quantity,
        NTILE(3) OVER (PARTITION BY category ORDER BY quantity, id) AS quantity_bucket,
        AVG(cost) OVER w_part AS cost_avg_5
    FROM with_latest
    WINDOW w_part AS (PARTITION BY supplier_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
)
SELECT
    brand,
    MAX(cost_avg_5) FILTER (WHERE category = 'Home') AS home_max_cost_avg_5,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Initech' THEN 1 END), 0), 2) AS stock_qty_per_initech,
    SIZE(COLLECT_SET(brand)) AS num_brands
FROM with_windows
GROUP BY brand
HAVING MAX(cost_avg_5) FILTER (WHERE category = 'Home') >= 0;
