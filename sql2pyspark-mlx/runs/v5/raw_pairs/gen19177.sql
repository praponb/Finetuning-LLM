WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        tags,
        supplier_id,
        FIRST_VALUE(cost, TRUE) OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        NTILE(5) OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) AS cost_bucket
    FROM products
),
kept_keys AS (
    -- Set difference on keys
    SELECT supplier_id, category
    FROM enriched
    EXCEPT ALL
    SELECT supplier_id, category
    FROM products
    WHERE supplier_id IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.supplier_id = k.supplier_id AND s.category = k.category
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, SORT_ARRAY(tags) AS sorted_tags
    FROM verified
    WHERE supplier_id IS NOT NULL
),
enriched_products AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY supplier_id, brand ORDER BY cost_bucket NULLS FIRST) AS cost_bucket_cume_dist,
        PERCENT_RANK() OVER (ORDER BY cost_bucket DESC) AS cost_bucket_percent_rank,
        CORR(stock_qty, price) OVER (PARTITION BY category) AS corr_stock_qty_price
    FROM filtered
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        cost_bucket_cume_dist
    FROM enriched_products
) src
PIVOT (
    SUM(cost_bucket_cume_dist) AS amount,
    COUNT(1) AS n
    FOR category IN ('Grocery', 'Sports')
)
