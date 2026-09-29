WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        units,
        ADD_MONTHS(sale_date, 1) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-02-12')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(units, 1) AS units_rounded,
        COALESCE(units, 0) AS units_filled
    FROM cleaned
    WHERE sale_date_plus_months >= '2024-11-01'
),
valid_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(sale_date_plus_months, 'yyyy-MM-dd') AS sale_date_plus_months_label
    FROM valid
    WHERE product_id IN (2, 7)
),
enriched AS (
    -- Attach products attributes
    SELECT s.*, p.category
    FROM valid_store_sales s
    LEFT JOIN products p
        ON s.product_id = p.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        sale_date,
        sale_date_plus_months,
        sale_date_plus_months_label,
        category
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category, product_id ORDER BY units_filled, id) = 1
),
ranked_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sale_date_plus_months NULLS LAST, id) AS rn
        FROM ranked
    ) t
    WHERE rn = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', sale_date_plus_months) AS quarter_start,
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    product_id,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list_2,
    COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS electronics_count,
    SIZE(COLLECT_SET(category)) AS num_categories
FROM ranked_store_sales
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', sale_date_plus_months), DATE_TRUNC('MONTH', sale_date), product_id), (DATE_TRUNC('QUARTER', sale_date_plus_months), DATE_TRUNC('MONTH', sale_date)))
ORDER BY activity_month DESC
