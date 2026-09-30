WITH kept_keys AS (
    SELECT id
    FROM inventory
    INTERSECT ALL
    SELECT id
    FROM inventory
    WHERE product_id IN (1, 5, 6, 8)
),
kept_rows AS (
    SELECT s.*
    FROM inventory s
    INNER JOIN kept_keys k ON s.id = k.id
),
cleaned AS (
    SELECT *, ADD_MONTHS(last_restock, 1) AS last_restock_plus_months
    FROM kept_rows
    WHERE last_restock > DATE '2023-06-20'
      AND quantity > 101
),
kept_keys_2 AS (
    SELECT store_id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT store_id
    FROM inventory
    WHERE YEAR(last_restock) = 2025
),
kept_rows_inventory AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys_2 k ON s.store_id = k.store_id
),
enriched AS (
    SELECT
        src.id,
        src.product_id,
        src.quantity,
        src.last_restock_plus_months,
        p.stock_qty,
        p.brand,
        p.launch_date
    FROM kept_rows_inventory src
    LEFT JOIN products p
        ON src.product_id = p.id
),
joined AS (
    SELECT
        a.product_id,
        a.quantity,
        a.last_restock_plus_months,
        p.cost,
        p.launch_date AS product_launch_date,
        p.brand AS product_brand
    FROM enriched a
    LEFT JOIN products p
        ON a.product_id = p.id
    WHERE p.product_name NOT LIKE 'Y%'
),
joined_inventory AS (
    SELECT
        src.product_id,
        src.quantity,
        src.last_restock_plus_months,
        src.cost,
        src.product_brand,
        p.cost AS product_cost,
        p.stock_qty,
        p.price
    FROM joined src
    INNER JOIN products p
        ON src.product_id = p.id
),
enriched_inventory AS (
    SELECT e.*, d.brand
    FROM joined_inventory e
    INNER JOIN products d
        ON e.product_id = d.id
)
SELECT
    product_id,
    last_restock_plus_months,
    product_brand,
    product_cost,
    price,
    brand,
    CASE WHEN product_brand RLIKE '^(Acme|Initech)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS product_brand_zone
FROM enriched_inventory
ORDER BY product_cost DESC, product_brand DESC;
