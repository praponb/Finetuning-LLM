WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(sale_date, 79) sale_date_minus_79d,
        units + revenue revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-02-24'
      AND sale_date < '2024-12-01'
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.sale_date,
        src.units,
        src.revenue,
        src.sale_date_minus_79d,
        p.launch_date,
        p.brand
    FROM base src
    INNER JOIN products p
        ON src.product_id = p.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM enriched
    EXCEPT ALL
    SELECT id, product_id
    FROM store_sales
    WHERE product_id IN (1, 6, 7, 10)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT a.*, d.category
    FROM verified a
    LEFT JOIN products d
        ON a.product_id = d.id
)
SELECT
    id,
    units,
    revenue,
    sale_date_minus_79d,
    launch_date,
    brand,
    category
FROM enriched_store_sales
ORDER BY launch_date DESC;
