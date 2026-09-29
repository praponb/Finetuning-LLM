WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(category, 1, 4) AS category_prefix,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'eco')
),
joined AS (
    -- Attach suppliers attributes
    SELECT s.*, p.city
    FROM cleaned s
    LEFT JOIN suppliers p
        ON s.supplier_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cost) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        CORR(price, stock_qty) OVER (PARTITION BY supplier_id, brand) AS corr_price_stock_qty,
        AVG(stock_qty) OVER (PARTITION BY brand, city ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_6_prior
    FROM joined
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_last_restock,
        x.inventory_quantity
    FROM metrics s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN category IN ('Fashion', 'Grocery') THEN 'Core' ELSE 'Other' END AS category_group,
        COALESCE(brand, 'Unknown') AS brand_filled,
        COALESCE(brand, 'UNKNOWN') AS brand_clean_2
    FROM with_last
    WHERE supplier_id IS NOT NULL
      AND stock_qty_avg_6_prior >= 208
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT e.*, p.city AS supplier_city
    FROM valid e
    INNER JOIN suppliers p
        ON e.supplier_id = p.id
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    tags,
    supplier_id,
    city,
    running_cost,
    CASE WHEN category RLIKE '^(Electronics|Fashion)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS category_zone
FROM with_dims
ORDER BY id DESC NULLS FIRST;
