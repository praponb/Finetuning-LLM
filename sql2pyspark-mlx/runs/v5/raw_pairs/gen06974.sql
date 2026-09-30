WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(category) AS category_upper
    FROM products
    WHERE brand IS NOT NULL
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.unit_price
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        cost - stock_qty AS cost_minus_stock_qty,
        ROUND(price * 100, 2) AS price_adjusted
    FROM latest_match
    WHERE stock_qty BETWEEN 144 AND 382
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        e.id,
        e.category,
        e.brand,
        e.cost,
        e.stock_qty,
        e.launch_date,
        e.tags,
        e.supplier_id,
        e.unit_price,
        e.cost_minus_stock_qty,
        e.price_adjusted,
        p.supplier_name
    FROM cleaned e
    LEFT JOIN suppliers p
        ON e.supplier_id <=> p.id
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.brand,
        s.stock_qty,
        s.launch_date,
        s.supplier_id,
        s.price_adjusted,
        s.supplier_name,
        x.store_sale_sale_date,
        x.revenue
    FROM joined s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        stock_qty,
        supplier_id,
        price_adjusted,
        store_sale_sale_date,
        COUNT(revenue) OVER (PARTITION BY brand ORDER BY store_sale_sale_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_revenue,
        MIN(revenue) OVER (PARTITION BY brand ORDER BY store_sale_sale_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_revenue,
        AVG(revenue) OVER (PARTITION BY brand ORDER BY store_sale_sale_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_revenue,
        MAX(revenue) OVER (PARTITION BY brand ORDER BY store_sale_sale_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_revenue,
        AVG(revenue) OVER (ORDER BY price_adjusted ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior
    FROM with_latest
    WHERE store_sale_sale_date < '2024-12-07'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock < s.store_sale_sale_date
        )
)
SELECT
    id,
    supplier_id,
    last4_count_revenue,
    last4_min_revenue,
    last4_avg_revenue,
    last4_max_revenue,
    revenue_avg_3_prior
FROM not_flagged
ORDER BY revenue_avg_3_prior ASC NULLS FIRST;
