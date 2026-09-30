WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(last_restock, 17) AS last_restock_plus_17d
    FROM inventory
    WHERE last_restock > DATE '2023-03-18'
      AND product_id IN (6, 7, 8, 9)
),
joined AS (
    -- Attach stores attributes
    SELECT s.*, d.sqft
    FROM base s
    JOIN stores d
        ON s.store_id = d.id
),
base_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        sqft,
        ADD_MONTHS(last_restock_plus_17d, 12) AS last_restock_plus_17d_plus_months,
        DATE_TRUNC('week', last_restock) AS last_restock_period
    FROM joined
    WHERE product_id = 2
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        sqft,
        DATEDIFF(CURRENT_DATE(), last_restock_plus_17d_plus_months) AS days_since_last_restock_plus_17d_plus_months,
        DATE_TRUNC('year', last_restock) AS last_restock_period_2
    FROM base_inventory
    WHERE product_id IN (4, 6, 9)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock ASC NULLS FIRST, id ASC) = 1
)
-- Grouped metrics
SELECT
    product_id,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(*) AS num_rows,
    SUM(sqft) AS total_sqft
FROM ranked
GROUP BY product_id
ORDER BY product_id DESC
