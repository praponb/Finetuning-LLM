WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(revenue) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
        CORR(revenue, units) OVER (PARTITION BY product_id, store_id) AS corr_revenue_units,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units DESC NULLS FIRST) AS units_percent_rank
    FROM store_sales
),
matched_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM scored
    EXCEPT
    SELECT store_id
    FROM store_sales
    WHERE MONTH(sale_date) IN (1, 4, 11)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.store_id = k.store_id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(last_known_revenue, 2) AS last_known_revenue_rounded,
        ABS(corr_revenue_units) AS abs_corr_revenue_units
    FROM reconciled
    WHERE sale_date >= DATE '2024-03-10'
),
valid_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        corr_revenue_units,
        last_known_revenue_rounded,
        abs_corr_revenue_units,
        units - revenue AS revenue_per_unit
    FROM valid
    WHERE sale_date > DATE '2024-03-17'
      AND store_id IN (2, 3, 5)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        product_id,
        ROUND(AVG(corr_revenue_units), 2) AS avg_corr_revenue_units,
        PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
        PERCENTILE_APPROX(last_known_revenue_rounded, 0.95) AS last_known_revenue_rounded_p95
    FROM valid_store_sales
    GROUP BY product_id
)
SELECT
    s.product_id,
    s.avg_corr_revenue_units,
    s.revenue_quartiles,
    s.last_known_revenue_rounded_p95,
    (SELECT MAX(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS max_inventory_quantity
FROM stats s
ORDER BY max_inventory_quantity ASC
