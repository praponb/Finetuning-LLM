WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        ROUND(stock_qty / cost, 2) AS stock_qty_per_cost,
        CASE WHEN brand = 'Initech' THEN 1 ELSE 0 END AS is_initech,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM products
    WHERE launch_date > DATE '2024-04-11'
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.inventory_quantity,
        latest.last_restock
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest_products AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.inventory_last_restock,
        x.quantity
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Running and moving aggregates
SELECT
    *,
    LEAD(cost, 1) OVER (ORDER BY cost, id) AS next_cost,
    SUM(is_initech) OVER (PARTITION BY category ORDER BY inventory_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_initech,
    AVG(inventory_quantity) OVER (PARTITION BY supplier_id, category ORDER BY stock_qty_per_cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS inventory_quantity_avg_2,
    SUM(is_initech) OVER (PARTITION BY supplier_id, category ORDER BY inventory_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_initech
FROM with_latest_products;
