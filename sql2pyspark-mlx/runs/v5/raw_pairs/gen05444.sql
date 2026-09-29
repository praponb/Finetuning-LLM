WITH base AS (
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        launch_date,
        supplier_id,
        price * cost AS margin,
        ARRAY_CONTAINS(tags, 'clearance') AS is_clearance
    FROM products
    WHERE brand IS NOT NULL
      AND launch_date > DATE '2023-05-02'
),
with_last AS (
    SELECT
        s.id,
        s.category,
        s.price,
        s.cost,
        s.supplier_id,
        s.margin,
        s.is_clearance,
        top1.unit_price,
        top1.quantity
    FROM base s
    JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        id,
        category,
        price,
        cost,
        supplier_id,
        margin,
        quantity,
        MAX(quantity) OVER (PARTITION BY supplier_id) AS supplier_id_max_quantity,
        LAG(unit_price, 1) OVER (PARTITION BY supplier_id ORDER BY margin ASC, id ASC) AS prev_unit_price,
        SUM(price) OVER (ORDER BY margin ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price
    FROM with_last
    WHERE is_clearance
),
base_products AS (
    SELECT
        id,
        category,
        margin,
        supplier_id_max_quantity,
        prev_unit_price,
        prev_unit_price * supplier_id_max_quantity AS prev_unit_price_times_supplier_id_max_quantity,
        CAST(prev_unit_price AS INT) AS prev_unit_price_int
    FROM metrics
    WHERE supplier_id IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM base_products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (7, 11, 12, 26)
        )
)
SELECT
    CASE WHEN supplier_id_max_quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS supplier_id_max_quantity_tier,
    CASE WHEN prev_unit_price_int >= 61 THEN 'HIGH' ELSE 'LOW' END AS prev_unit_price_int_tier,
    ROUND(SUM(prev_unit_price_int) / NULLIF(COUNT(CASE WHEN category = 'Home' THEN 1 END), 0), 2) AS prev_unit_price_int_per_home,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(supplier_id_max_quantity) AS max_supplier_id_max_quantity,
    ROUND(SUM(margin) / NULLIF(COUNT(CASE WHEN category = 'Fashion' THEN 1 END), 0), 2) AS margin_per_fashion
FROM kept
GROUP BY CUBE (CASE WHEN supplier_id_max_quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_unit_price_int >= 61 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY supplier_id_max_quantity_tier DESC;
