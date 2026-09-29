WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC NULLS LAST, id) AS row_num
    FROM products
    QUALIFY row_num = 1
),
not_flagged AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id IS NOT NULL
        )
),
base AS (
    SELECT
        id,
        product_name,
        category,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        row_num,
        price + stock_qty AS price_plus_stock_qty,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM not_flagged
    WHERE launch_date >= DATE '2023-05-21'
      AND supplier_id IS NOT NULL
),
ranked_products AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, category ORDER BY price NULLS LAST, id) AS latest_rank
        FROM base
    ) x
    WHERE latest_rank = 1
),
with_last AS (
    SELECT
        s.id,
        s.category,
        s.stock_qty,
        s.launch_date,
        s.supplier_id,
        s.row_num,
        s.brand_filled,
        s.latest_rank,
        x.order_item_quantity
    FROM ranked_products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    SELECT
        s.id,
        s.stock_qty,
        s.supplier_id,
        s.row_num,
        s.order_item_quantity,
        p2.supplier_name,
        p2.city,
        p2.country
    FROM with_last s
    LEFT JOIN suppliers p2
        ON s.supplier_id = p2.id
)
SELECT
    id,
    order_item_quantity,
    city,
    country
FROM joined
ORDER BY country NULLS LAST;
