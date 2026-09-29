WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC NULLS FIRST, id) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), sale_date) days_since_sale_date,
        ROUND(revenue, 1) revenue_rounded
    FROM latest
    WHERE sale_date >= DATE '2023-08-20'
),
valid_store_sales AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(revenue_rounded / units, 2) revenue_rounded_per_units
    FROM valid
    WHERE sale_date >= TO_DATE('2023-04-20')
),
joined AS (
    -- Attach products attributes
    SELECT src.*, p.price
    FROM valid_store_sales src
    JOIN products p
        ON src.product_id = p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY days_since_sale_date DESC NULLS FIRST, id) = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT product_id, id
    FROM deduped
    INTERSECT
    SELECT product_id, id
    FROM store_sales
    WHERE store_id IN (2, 4, 5, 6)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN matched_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    s.store_id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.days_since_sale_date,
    s.revenue_rounded,
    s.price,
    (SELECT SUM(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS sum_inventory_quantity
FROM verified s
ORDER BY sale_date DESC;
