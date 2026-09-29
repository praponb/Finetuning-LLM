WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id) = 1
),
enriched AS (
    -- Attach products attributes
    SELECT e.*, b.brand
    FROM ranked e
    LEFT JOIN products b
        ON e.product_id = b.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
ranked_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(units) OVER (PARTITION BY store_id) AS store_id_sum_units,
        ROUND(units / SUM(units) OVER (PARTITION BY product_id), 4) AS units_share
    FROM ranked_store_sales
    WHERE revenue >= 1500
)
-- Join a dimension and aggregate
SELECT
    p.category,
    p.brand AS product_brand,
    COUNT(*) AS num_rows,
    SUM(a.revenue) AS total_revenue,
    MIN(a.units_share) AS min_units_share
FROM enriched_store_sales a
INNER JOIN products p ON a.product_id = p.id
GROUP BY GROUPING SETS ((p.category, p.brand), (p.category), ())
ORDER BY product_brand DESC;
