WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(units * 0.85, 2) AS units_adjusted,
        COALESCE(units, 0) AS units_filled
    FROM store_sales
    WHERE store_id IN (1, 2)
),
valid_store_sales AS (
    SELECT
        id,
        store_id,
        product_id,
        units,
        units_adjusted,
        units_filled,
        ROUND(units_adjusted * 0.85, 2) AS units_adjusted_scaled,
        CEIL(units_filled) AS units_filled_whole
    FROM valid
    WHERE sale_date >= DATE '2023-05-02'
      AND store_id IN (1, 4)
),
kept_keys AS (
    SELECT product_id
    FROM valid_store_sales
    INTERSECT ALL
    SELECT product_id
    FROM store_sales
    WHERE revenue <= 4000
),
reconciled AS (
    SELECT s.*
    FROM valid_store_sales s
    JOIN kept_keys k ON s.product_id = k.product_id
),
kept_keys_2 AS (
    SELECT product_id, id
    FROM reconciled
    EXCEPT
    SELECT product_id, id
    FROM store_sales
    WHERE sale_date < '2023-08-09'
),
kept_rows AS (
    SELECT s.*
    FROM reconciled s
    JOIN kept_keys_2 k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    product_id,
    units_filled,
    units_adjusted_scaled,
    units_filled_whole
FROM kept_rows
ORDER BY product_id
