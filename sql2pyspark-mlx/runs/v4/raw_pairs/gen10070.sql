WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        units,
        revenue,
        DAYOFWEEK(sale_date) AS sale_date_dayofweek
    FROM store_sales
    WHERE sale_date >= TO_DATE('2024-03-13')
      AND sale_date >= '2023-07-01'
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.category,
        p.launch_date
    FROM valid a
    LEFT JOIN products p
        ON a.product_id = p.id
),
joined_store_sales AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.stock_qty,
        p.product_name,
        p.launch_date AS product_launch_date
    FROM joined src
    INNER JOIN products p
        ON src.product_id = p.id
)
SELECT
    id,
    units,
    revenue,
    category,
    product_launch_date
FROM joined_store_sales
