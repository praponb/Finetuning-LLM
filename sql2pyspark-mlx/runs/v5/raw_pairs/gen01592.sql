WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        ROUND(units, 0) AS units_rounded
    FROM store_sales
    WHERE sale_date >= DATE '2023-09-30'
      AND units = 37
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.units_rounded,
        p.launch_date
    FROM base e
    INNER JOIN products p
        ON e.product_id = p.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units_rounded NULLS FIRST, id) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
),
joined_store_sales AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.units_rounded,
        s.launch_date,
        d.brand
    FROM ranked s
    LEFT JOIN products d
        ON s.product_id = d.id
    WHERE d.product_name LIKE '%Backpack%'
),
enriched AS (
    -- Attach products attributes
    SELECT src.*, b.cost
    FROM joined_store_sales src
    INNER JOIN products b
        ON src.product_id = b.id
)
-- Grouped metrics
SELECT
    product_id,
    brand,
    COUNT(*) AS row_count,
    COUNT(*) AS event_count
FROM enriched
GROUP BY product_id, brand;
