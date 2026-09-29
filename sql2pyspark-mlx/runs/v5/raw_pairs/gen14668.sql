WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        LAST_DAY(launch_date) launch_date_month_end
    FROM products
    WHERE launch_date >= DATE '2023-08-24'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cost) OVER (ORDER BY launch_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_cost,
        NTILE(10) OVER (PARTITION BY category ORDER BY stock_qty, id) stock_qty_bucket,
        LAG(cost, 1) OVER (ORDER BY stock_qty, id) prev_cost
    FROM base
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY running_cost DESC NULLS FIRST, id) AS rn
    FROM enriched
    QUALIFY rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        cost,
        stock_qty,
        stock_qty_bucket,
        prev_cost,
        SUM(cost) OVER (PARTITION BY brand) brand_sum_cost,
        AVG(running_cost) OVER (ORDER BY rn, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) running_cost_avg_6
    FROM deduped
    WHERE cost <= 520
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        cost,
        stock_qty,
        stock_qty_bucket,
        prev_cost,
        brand_sum_cost,
        running_cost_avg_6,
        CASE WHEN stock_qty >= 435 THEN 'High' WHEN stock_qty >= 77 THEN 'Medium' ELSE 'Low' END stock_qty_level
    FROM scored
    WHERE cost <= 300
)
SELECT
    id,
    category,
    stock_qty,
    prev_cost,
    brand_sum_cost,
    running_cost_avg_6,
    stock_qty_level,
    CASE WHEN category RLIKE '^(Fashion|Electronics)$' THEN 'GROUP_B' WHEN category RLIKE '^(Home|Sports)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS category_zone
FROM cleaned
ORDER BY running_cost_avg_6 DESC NULLS LAST, category_zone;
