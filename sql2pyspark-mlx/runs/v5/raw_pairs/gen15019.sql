WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, WEEKOFYEAR(last_restock) AS last_restock_weekofyear
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-04-23')
      AND product_id IN (4, 7)
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        b.stock_qty,
        b.product_name
    FROM valid a
    INNER JOIN products b
        ON a.product_id = b.id
    WHERE LENGTH(b.product_name) >= 13
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.stock_qty,
        b.opened_date,
        b.city,
        b.sqft
    FROM joined s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
valid_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        stock_qty,
        opened_date,
        city,
        sqft,
        ROUND(sqft, 1) AS sqft_rounded,
        COALESCE(city, 'NONE') AS resolved_city
    FROM with_dims
    WHERE city IS NOT DISTINCT FROM 'Chiang Mai'
)
SELECT
    id,
    product_id,
    stock_qty,
    opened_date,
    city,
    sqft,
    resolved_city
FROM valid_inventory
ORDER BY product_id NULLS FIRST
