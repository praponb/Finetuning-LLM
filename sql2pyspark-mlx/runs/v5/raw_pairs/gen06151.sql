WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_ADD(last_restock, 25) AS last_restock_plus_25d,
        QUARTER(last_restock) AS last_restock_quarter
    FROM inventory
    WHERE last_restock >= DATE '2023-07-08'
),
valid_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        last_restock,
        last_restock_plus_25d,
        last_restock_quarter,
        COALESCE(quantity, 0) AS quantity_filled,
        ROUND(last_restock_quarter, 2) AS last_restock_quarter_rounded
    FROM valid
    WHERE last_restock_quarter < 43
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(quantity_filled) IGNORE NULLS OVER (ORDER BY last_restock_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_filled,
        LAST_VALUE(quantity_filled, TRUE) OVER (ORDER BY last_restock_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_filled,
        FIRST_VALUE(last_restock_quarter, TRUE) OVER (ORDER BY last_restock_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_restock_quarter,
        NTILE(5) OVER (ORDER BY last_restock_quarter DESC, id) AS last_restock_quarter_bucket,
        NTILE(4) OVER (ORDER BY last_restock_quarter_rounded, id) AS last_restock_quarter_rounded_bucket
    FROM valid_inventory
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (ORDER BY last_restock_quarter DESC, id) AS last_restock_quarter_bucket_2,
        NTILE(10) OVER (ORDER BY first_known_quantity_filled, id) AS first_known_quantity_filled_bucket
    FROM with_windows
    WHERE last_restock_quarter > 44
)
SELECT
    id,
    last_restock,
    last_restock_plus_25d,
    last_restock_quarter,
    last_restock_quarter_rounded,
    first_known_quantity_filled,
    first_known_last_restock_quarter,
    last_restock_quarter_bucket,
    last_restock_quarter_rounded_bucket
FROM enriched
ORDER BY last_restock_quarter_rounded DESC;
