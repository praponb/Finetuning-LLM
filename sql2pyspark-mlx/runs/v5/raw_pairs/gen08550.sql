WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC NULLS LAST, id) AS row_num
        FROM store_sales
    ) t
    WHERE row_num = 1
),
enriched AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY units, id) AS event_seq,
        LAST_VALUE(units) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units,
        LAG(revenue, 2) OVER (ORDER BY sale_date, id) AS prev_revenue,
        CUME_DIST() OVER (ORDER BY units NULLS LAST) AS units_cume_dist
    FROM deduped
),
deduped_store_sales AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC, id) = 1
),
enriched_store_sales AS (
    SELECT
        e.id,
        e.product_id,
        e.sale_date,
        e.revenue,
        e.revenue_avg_3_prior,
        e.units_cume_dist,
        p.category,
        p.cost,
        p.brand
    FROM deduped_store_sales e
    INNER JOIN products p
        ON e.product_id = p.id
    WHERE p.supplier_id IS NULL
)
SELECT
    id,
    revenue,
    revenue_avg_3_prior,
    units_cume_dist,
    category,
    cost,
    brand
FROM enriched_store_sales
ORDER BY category
