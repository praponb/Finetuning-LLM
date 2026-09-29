WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN units >= 44 THEN 'High' WHEN units >= 8 THEN 'Medium' ELSE 'Low' END AS units_tier,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date >= '2023-03-01'
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        src.revenue,
        src.sale_date_month_end,
        p.opened_date,
        p.sqft,
        p.store_name
    FROM cleaned src
    INNER JOIN stores p
        ON src.store_id = p.id
    WHERE p.sqft BETWEEN 12996 AND 13700
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.city,
        s.sqft AS store_sqft,
        s.store_name AS store_store_name
    FROM enriched a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(sqft) OVER w_running AS next_sqft,
        LEAD(store_sqft) OVER w_running AS next_store_sqft,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM enriched_store_sales
    WINDOW w_running AS (PARTITION BY product_id ORDER BY sale_date_month_end, id)
)
-- Join a dimension and aggregate
SELECT
    s.city AS store_city,
    a.city AS store_sale_city,
    COUNT(*) AS rows_total,
    MAX(a.next_store_sqft) AS max_next_store_sqft
FROM metrics a
LEFT JOIN stores s ON a.store_id = s.id
GROUP BY GROUPING SETS ((s.city, a.city), (s.city), ());
