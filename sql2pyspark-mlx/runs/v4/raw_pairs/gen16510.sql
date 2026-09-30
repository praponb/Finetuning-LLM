WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(stock_qty / price, 2) stock_qty_per_price,
        COALESCE(brand, 'N/A') resolved_brand
    FROM products
    WHERE supplier_id IS NOT NULL
      AND category IN ('Electronics', 'Fashion', 'Books')
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_quantity,
        x.last_restock
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.units > 40
        )
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        p.country,
        p.city
    FROM eligible src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        brand,
        city,
        APPROX_PERCENTILE(inventory_quantity, ARRAY(0.25, 0.5, 0.75)) AS inventory_quantity_quartiles,
        ROUND(SUM(stock_qty_per_price) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS stock_qty_per_price_per_berlin,
        COUNT_IF(brand = 'Umbrella') AS umbrella_events,
        ROUND(AVG(price), 2) AS avg_price,
        GROUPING(brand) AS grouping_level
    FROM with_dims
    GROUP BY GROUPING SETS ((brand, city), (brand))
    HAVING stock_qty_per_price_per_berlin > 0
)
SELECT
    inventory_quantity_quartiles,
    stock_qty_per_price_per_berlin,
    avg_price
FROM totals
ORDER BY avg_price;
