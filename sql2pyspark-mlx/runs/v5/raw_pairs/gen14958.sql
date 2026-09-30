WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(price / SUM(price) OVER (PARTITION BY category, supplier_id), 4) AS price_share,
        CORR(price, stock_qty) OVER (PARTITION BY category) AS corr_price_stock_qty
    FROM products
)
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
    CASE WHEN product_name RLIKE '^(Office Chair|USB-C Hub)$' THEN 'OTHER_ZONE' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'EXTENDED' ELSE 'CORE' END AS product_name_zone
FROM metrics
ORDER BY price NULLS FIRST
