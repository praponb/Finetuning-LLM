WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(price) price_whole,
        COALESCE(brand, 'UNKNOWN') brand_filled
    FROM products
    WHERE launch_date > DATE '2023-04-30'
      AND supplier_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(stock_qty) OVER (PARTITION BY brand, supplier_id) brand_sum_stock_qty,
        SUM(stock_qty) OVER w_part running_stock_qty,
        AVG(price_whole) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) price_whole_avg_6
    FROM base
    WHERE MONTH(launch_date) IN (4, 8, 10)
    WINDOW w_part AS (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        category,
        price,
        stock_qty,
        price_whole,
        brand_sum_stock_qty,
        running_stock_qty,
        MIN(price) OVER (PARTITION BY category ORDER BY running_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_price,
        MAX(price) OVER (PARTITION BY category ORDER BY running_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_price,
        SUM(price) OVER (PARTITION BY category ORDER BY running_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_price,
        MAX(brand_sum_stock_qty) OVER (PARTITION BY category) category_max_brand_sum_stock_qty
    FROM scored
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.stock_qty,
        s.brand_sum_stock_qty,
        s.last3_min_price,
        s.last3_max_price,
        s.category_max_brand_sum_stock_qty,
        top1.last_restock,
        top1.inventory_quantity
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    category,
    stock_qty,
    brand_sum_stock_qty,
    last3_min_price,
    last3_max_price,
    last_restock,
    CASE WHEN category RLIKE '^(Books|Fashion)$' THEN 'GROUP_A' WHEN category RLIKE '^(Electronics|Home)$' THEN 'PRIORITY' ELSE 'CORE' END AS category_zone
FROM with_latest
ORDER BY id DESC
