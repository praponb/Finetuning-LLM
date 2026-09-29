WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        DAYOFWEEK(launch_date) AS launch_date_dayofweek,
        COALESCE(brand, 'DIRECT') AS brand_clean
    FROM products
    WHERE launch_date > TO_DATE('2024-03-04')
      AND supplier_id IS NOT NULL
      AND stock_qty > 313
)
-- Running and moving aggregates
SELECT
    *,
    ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share,
    NTILE(5) OVER w_recent AS cost_bucket
FROM valid
WINDOW w_recent AS (PARTITION BY brand ORDER BY cost ASC, id ASC);
