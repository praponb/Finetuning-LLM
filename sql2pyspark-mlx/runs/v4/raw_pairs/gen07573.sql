WITH scored AS (
    SELECT
        *,
        COUNT(cost) OVER (PARTITION BY brand) brand_count_cost,
        FIRST_VALUE(cost, TRUE) OVER (PARTITION BY category ORDER BY price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_cost,
        NTILE(5) OVER (PARTITION BY category, brand ORDER BY cost ASC, id ASC) cost_bucket
    FROM products
),
metrics AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_count_cost,
        first_known_cost,
        cost_bucket,
        NTILE(4) OVER (PARTITION BY supplier_id ORDER BY stock_qty ASC, id ASC) stock_qty_bucket,
        STDDEV_SAMP(brand_count_cost) OVER (PARTITION BY category) category_stddev_samp_brand_count_cost,
        ROUND(brand_count_cost / SUM(brand_count_cost) OVER (PARTITION BY category, supplier_id), 4) brand_count_cost_share,
        FIRST_VALUE(cost_bucket) IGNORE NULLS OVER (PARTITION BY brand ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_cost_bucket,
        CORR(price, cost) OVER (PARTITION BY brand) corr_price_cost
    FROM scored
),
matched_keys AS (
    SELECT brand, category
    FROM metrics
    EXCEPT ALL
    SELECT brand, category
    FROM products
    WHERE supplier_id IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.brand = k.brand AND s.category = k.category
),
enriched AS (
    SELECT
        *,
        AVG(category_stddev_samp_brand_count_cost) OVER (PARTITION BY category) category_avg_category_stddev_samp_brand_count_cost,
        RANK() OVER (PARTITION BY supplier_id ORDER BY cost_bucket DESC NULLS FIRST) cost_bucket_rank
    FROM reconciled
),
kept_keys AS (
    SELECT brand, supplier_id
    FROM enriched
    INTERSECT
    SELECT brand, supplier_id
    FROM products
    WHERE brand IN ('Initech', 'Globex')
),
verified AS (
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.brand = k.brand AND s.supplier_id = k.supplier_id
),
daily AS (
    SELECT
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
        ROUND(SUM(cost_bucket_rank) / NULLIF(COUNT(CASE WHEN category = 'Books' THEN 1 END), 0), 2) AS cost_bucket_rank_per_books,
        AVG(cost_bucket) AS avg_cost_bucket,
        COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM verified
    GROUP BY DATE_TRUNC('WEEK', launch_date)
)
SELECT
    week_start,
    cost_bucket_rank_per_books,
    acme_count
FROM daily
ORDER BY acme_count DESC
