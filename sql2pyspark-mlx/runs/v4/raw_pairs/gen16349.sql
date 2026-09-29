WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        stock_qty - price AS stock_qty_minus_price,
        CASE WHEN brand IN ('Stark', 'Initech') THEN 'Tier 1' ELSE 'Other' END AS brand_group,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_quantity
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Keep recent, valid rows
    SELECT *, cost * stock_qty_minus_price AS cost_times_stock_qty_minus_price
    FROM latest_match
    WHERE stock_qty != 306
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_revenue,
        top1.store_sale_units
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.category,
    s.stock_qty,
    s.tags,
    s.supplier_id,
    s.stock_qty_minus_price,
    s.brand_group,
    s.order_item_quantity,
    s.store_sale_revenue,
    CASE WHEN category RLIKE '^(Fashion|Home)$' THEN 'EXTENDED' WHEN category RLIKE '^(Electronics|Sports)$' THEN 'GROUP_B' ELSE 'CORE' END AS category_zone,
    (SELECT AVG(p2.cost) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS avg_product_cost
FROM with_last s
ORDER BY store_sale_revenue DESC
