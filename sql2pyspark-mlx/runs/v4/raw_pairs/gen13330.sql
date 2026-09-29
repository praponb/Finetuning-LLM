WITH base AS (
    SELECT
        id,
        product_id,
        units,
        revenue,
        revenue - units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-06'
      AND units IS NOT NULL
),
cleaned AS (
    SELECT *, revenue * units AS revenue_per_unit_2
    FROM base
    WHERE revenue > 1200
),
enriched AS (
    SELECT
        e.*,
        b.launch_date,
        b.category,
        b.price
    FROM cleaned e
    INNER JOIN products b
        ON e.product_id = b.id
),
enriched_store_sales AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id, category ORDER BY revenue DESC, id) AS row_seq,
        AVG(revenue_per_unit_2) OVER (PARTITION BY category, product_id ORDER BY revenue_per_unit_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_per_unit_2_avg_6
    FROM enriched
)
SELECT
    id,
    units,
    revenue,
    launch_date,
    category,
    row_seq,
    CASE WHEN category RLIKE '^(Grocery|Fashion)$' THEN 'PRIORITY' WHEN category RLIKE '^(Electronics|Home)$' THEN 'CORE' ELSE 'GROUP_B' END AS category_zone
FROM enriched_store_sales
