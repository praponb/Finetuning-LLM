WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, revenue * units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-05-20'
      AND sale_date BETWEEN '2023-05-01' AND '2024-01-09'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id
    FROM valid
    INTERSECT ALL
    SELECT store_id
    FROM store_sales
    WHERE store_id IN (1, 3, 6)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.store_id = k.store_id
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.product_id,
        a.sale_date,
        a.units,
        a.revenue,
        a.revenue_per_unit,
        p.category,
        p.brand,
        p.product_name
    FROM kept_rows a
    JOIN products p
        ON a.product_id = p.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM with_dims
    INTERSECT
    SELECT product_id
    FROM store_sales
    WHERE store_id IN (1, 3, 4, 5)
),
kept_rows_store_sales AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN matched_keys k ON s.product_id = k.product_id
),
with_dims_store_sales AS (
    -- Attach products attributes
    SELECT
        e.product_id,
        e.revenue,
        e.revenue_per_unit,
        e.brand,
        e.product_name,
        p.product_name AS product_product_name,
        p.cost
    FROM kept_rows_store_sales e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE p.price <= 530
),
daily AS (
    -- Multi-level aggregation
    SELECT
        product_id,
        brand,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(revenue_per_unit, 0.9) AS revenue_per_unit_p90,
        APPROX_PERCENTILE(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
        SIZE(COLLECT_SET(brand)) AS num_brands,
        ROUND(AVG(revenue), 2) AS avg_revenue,
        RANK() OVER (PARTITION BY product_id ORDER BY ROUND(AVG(revenue), 2) DESC) AS avg_revenue_rank
    FROM with_dims_store_sales
    GROUP BY 1, 2
)
SELECT
    product_id,
    brand,
    event_count,
    avg_revenue,
    avg_revenue_rank
FROM daily
