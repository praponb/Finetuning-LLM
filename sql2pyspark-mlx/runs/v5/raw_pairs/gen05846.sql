WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(revenue * 1.07, 2) AS revenue_adjusted
    FROM store_sales
    WHERE sale_date > DATE '2023-07-27'
      AND revenue >= 3400
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.sale_date,
        a.units,
        a.revenue_adjusted,
        d.store_name,
        d.region
    FROM cleaned a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM with_dims
    EXCEPT ALL
    SELECT product_id
    FROM store_sales
    WHERE store_id IN (1, 2, 4)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue_adjusted,
        store_name,
        region,
        GREATEST(units, revenue_adjusted) AS greatest_units_revenue_adjusted,
        UPPER(store_name) AS store_name_upper
    FROM reconciled
    WHERE region IS NOT NULL
      AND region IS NOT DISTINCT FROM 'North'
),
joined AS (
    -- Attach products attributes
    SELECT a.*, b.stock_qty
    FROM base a
    INNER JOIN products b
        ON a.product_id = b.id
),
with_dims_store_sales AS (
    -- Attach products attributes
    SELECT
        e.product_id,
        e.sale_date,
        e.revenue_adjusted,
        e.greatest_units_revenue_adjusted,
        e.store_name_upper,
        e.stock_qty,
        d.cost,
        d.stock_qty AS product_stock_qty,
        d.price
    FROM joined e
    LEFT JOIN products d
        ON e.product_id = d.id
    WHERE d.brand = 'Umbrella'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        product_id,
        sale_date,
        store_name_upper,
        cost,
        price,
        CUME_DIST() OVER (ORDER BY cost DESC) AS cost_cume_dist,
        RANK() OVER (ORDER BY revenue_adjusted DESC NULLS LAST) AS revenue_adjusted_rank,
        SUM(greatest_units_revenue_adjusted) OVER (PARTITION BY product_id) AS product_id_sum_greatest_units_revenue_adjusted,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY stock_qty) AS stock_qty_dense_rank,
        RANK() OVER (PARTITION BY product_id ORDER BY product_stock_qty NULLS FIRST) AS product_stock_qty_rank
    FROM with_dims_store_sales
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    CASE WHEN stock_qty_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS stock_qty_dense_rank_tier,
    SUM(cost) AS total_cost,
    COUNT(*) AS num_rows,
    COUNT(*) AS row_count
FROM enriched
GROUP BY DATE_TRUNC('YEAR', sale_date), CASE WHEN stock_qty_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
