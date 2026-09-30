WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        ADD_MONTHS(launch_date, 12) AS launch_date_plus_months,
        CASE WHEN category = 'Home' THEN 1 ELSE 0 END AS is_home
    FROM products
    WHERE launch_date >= DATE '2023-03-17'
      AND category IN ('Electronics', 'Home')
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.unit_price
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(unit_price) OVER (PARTITION BY brand) AS brand_avg_unit_price,
        AVG(price) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS price_avg_5,
        NTILE(10) OVER (ORDER BY cost DESC, id) AS cost_bucket,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date_plus_months DESC, id) AS event_seq,
        SUM(unit_price) OVER (PARTITION BY supplier_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price
    FROM with_latest
),
with_latest_products AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.quantity
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    supplier_id,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(is_home) FILTER (WHERE brand = 'Umbrella') AS umbrella_max_is_home,
    COUNT_IF(brand = 'Globex') AS globex_events,
    MAX(quantity) AS max_quantity,
    ROUND(SUM(is_home) / NULLIF(COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END), 0), 2) AS is_home_per_umbrella
FROM with_latest_products
GROUP BY supplier_id;
