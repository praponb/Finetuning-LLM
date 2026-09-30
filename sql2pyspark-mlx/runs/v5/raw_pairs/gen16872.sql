WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(quantity AS STRING) quantity_str
    FROM inventory
    WHERE last_restock >= '2024-12-01'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(quantity, 0) quantity_filled
    FROM filtered
    WHERE last_restock >= DATE '2023-04-27'
      AND quantity < 237
),
cleaned_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_str,
        quantity_filled,
        COALESCE(quantity, 0) quantity_filled_2
    FROM cleaned
    WHERE last_restock > DATE '2023-02-05'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        quantity + quantity_filled quantity_plus_quantity_filled,
        DATE_FORMAT(last_restock, 'MMM yyyy') last_restock_label
    FROM cleaned_inventory
    WHERE last_restock > DATE '2023-02-05'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) = 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        store_id,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(quantity_plus_quantity_filled, ARRAY(0.25, 0.5, 0.75)) AS quantity_plus_quantity_filled_quartiles,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        SUM(quantity) AS total_quantity,
        ROUND(AVG(quantity), 2) AS avg_quantity,
        PERCENTILE_APPROX(quantity_plus_quantity_filled, 0.5) AS quantity_plus_quantity_filled_p50,
        GROUPING(store_id) AS grouping_level
    FROM latest
    GROUP BY ROLLUP (store_id)
)
SELECT
    num_rows,
    quantity_plus_quantity_filled_quartiles,
    total_quantity,
    avg_quantity,
    grouping_level
FROM summary
ORDER BY avg_quantity DESC
