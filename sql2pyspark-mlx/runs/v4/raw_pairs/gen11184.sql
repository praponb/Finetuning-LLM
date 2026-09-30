WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE opened_date > '2025-03-01'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date >= '2025-09-01'
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST) AS dedup_rank
        FROM kept
    ) ranked
    WHERE dedup_rank = 1
),
kept_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date >= s.opened_date
        )
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    opened_date_label,
    region_clean,
    dedup_rank
FROM kept_stores
