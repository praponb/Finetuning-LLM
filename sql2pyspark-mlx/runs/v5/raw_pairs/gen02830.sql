WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        stock_qty,
        tags,
        UPPER(product_name) AS product_name_upper
    FROM products
    WHERE supplier_id IS NOT NULL
      AND SIZE(tags) > 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY stock_qty DESC, id ASC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(stock_qty) OVER w_part AS running_stock_qty,
        SUM(stock_qty) OVER (PARTITION BY category, brand) AS category_sum_stock_qty,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER w_part_2 AS last_known_stock_qty,
        SUM(stock_qty) OVER w_part AS running_stock_qty_2,
        AVG(stock_qty) OVER (PARTITION BY brand ORDER BY rn ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS stock_qty_avg_2
    FROM ranked
    WINDOW w_part AS (PARTITION BY category ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY brand, category ORDER BY stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_products AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        tags,
        product_name_upper,
        rn,
        category_sum_stock_qty,
        last_known_stock_qty,
        running_stock_qty_2,
        stock_qty_avg_2,
        SUM(running_stock_qty) OVER (PARTITION BY category, brand ORDER BY last_known_stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_stock_qty,
        ROW_NUMBER() OVER (ORDER BY running_stock_qty_2 ASC, id ASC) AS row_seq
    FROM scored
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        tags,
        product_name_upper,
        category_sum_stock_qty,
        last_known_stock_qty,
        row_seq,
        FIRST_VALUE(last_known_stock_qty) IGNORE NULLS OVER (ORDER BY row_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_known_stock_qty,
        AVG(cumulative_running_stock_qty) OVER (ORDER BY last_known_stock_qty ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_running_stock_qty_avg_6,
        FIRST_VALUE(cumulative_running_stock_qty) IGNORE NULLS OVER (ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_running_stock_qty,
        LAST_VALUE(last_known_stock_qty) IGNORE NULLS OVER (ORDER BY last_known_stock_qty ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_stock_qty,
        NTILE(4) OVER w_grp AS category_sum_stock_qty_bucket
    FROM scored_products
    WINDOW w_grp AS (ORDER BY category_sum_stock_qty DESC, id ASC)
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_cumulative_running_stock_qty >= 8696 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_cumulative_running_stock_qty_tier,
    CASE WHEN last_known_last_known_stock_qty >= 395 THEN 'LARGE' ELSE 'SMALL' END AS last_known_last_known_stock_qty_tier,
    CASE WHEN first_known_last_known_stock_qty >= 402 THEN 'LARGE' ELSE 'SMALL' END AS first_known_last_known_stock_qty_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(last_known_last_known_stock_qty), 2) AS avg_last_known_last_known_stock_qty,
    COUNT(*) AS row_count_2
FROM metrics
GROUP BY CASE WHEN first_known_cumulative_running_stock_qty >= 8696 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last_known_last_known_stock_qty >= 395 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN first_known_last_known_stock_qty >= 402 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(last_known_last_known_stock_qty), 2) > 0;
