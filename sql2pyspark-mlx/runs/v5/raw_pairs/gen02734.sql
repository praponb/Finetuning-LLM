WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        COALESCE(price, 0) price_filled,
        SORT_ARRAY(tags) sorted_tags
    FROM products
    WHERE launch_date >= TO_DATE('2023-05-30')
),
joined AS (
    -- Attach suppliers attributes
    SELECT s.*, d.rating
    FROM valid s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
    WHERE d.rating <= 3.7
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.unit_price,
        latest.order_item_quantity
    FROM joined s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.quantity
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.tags,
    s.supplier_id,
    (SELECT SUM(x.stock_qty) FROM products x WHERE x.supplier_id = s.supplier_id) AS sum_product_stock_qty
FROM latest_match s;
