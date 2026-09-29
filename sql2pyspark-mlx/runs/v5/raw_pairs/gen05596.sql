WITH base AS (
    SELECT *, SUBSTRING(category, 1, 2) AS category_prefix
    FROM products
    WHERE launch_date >= DATE '2023-05-15'
      AND supplier_id IS NOT NULL
),
deduped AS (
    SELECT
        id,
        category,
        stock_qty,
        supplier_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost NULLS LAST, id) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
with_last AS (
    SELECT
        s.id,
        s.supplier_id,
        latest.inventory_quantity,
        latest.last_restock
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last_products AS (
    SELECT
        s.id,
        s.supplier_id,
        s.last_restock,
        x.order_item_unit_price
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.supplier_id,
    (SELECT MAX(x.cost) FROM products x WHERE x.supplier_id = s.supplier_id) AS max_product_cost
FROM with_last_products s;
