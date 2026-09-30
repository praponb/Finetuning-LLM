WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        units,
        revenue,
        units - revenue AS revenue_per_unit
    FROM store_sales
    WHERE store_id IN (1, 4)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        revenue_per_unit,
        NTILE(5) OVER (ORDER BY revenue, id) AS revenue_bucket,
        LEAD(revenue, 1) OVER (PARTITION BY store_id ORDER BY units, id) AS next_revenue
    FROM base
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM scored
    EXCEPT ALL
    SELECT store_id, id
    FROM store_sales
    WHERE units BETWEEN 21 AND 24
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.opened_date,
        b.city,
        b.sqft
    FROM kept_rows e
    LEFT JOIN stores b
        ON e.store_id = b.id
)
SELECT
    id,
    revenue_per_unit,
    revenue_bucket,
    next_revenue,
    sqft
FROM enriched
ORDER BY sqft DESC;
