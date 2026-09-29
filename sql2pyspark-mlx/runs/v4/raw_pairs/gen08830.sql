WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN quantity >= 266 THEN 'High' WHEN quantity >= 38 THEN 'Medium' ELSE 'Low' END AS quantity_tier,
        DATE_ADD(last_restock, 52) AS last_restock_plus_52d
    FROM inventory
    WHERE last_restock >= DATE '2024-01-19'
      AND quantity BETWEEN 172 AND 230
),
filtered_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_label
    FROM filtered
    WHERE last_restock >= DATE '2023-03-06'
      AND last_restock_plus_52d <= '2024-12-01'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered_inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN quantity >= 193 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    product_id,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    BIT_AND(quantity) AS quantity_bit_and,
    AVG(quantity) AS avg_quantity,
    SUM(quantity) AS total_quantity
FROM latest
GROUP BY GROUPING SETS ((CASE WHEN quantity >= 193 THEN 'LARGE' ELSE 'SMALL' END, product_id), (CASE WHEN quantity >= 193 THEN 'LARGE' ELSE 'SMALL' END), ());
