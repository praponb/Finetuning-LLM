WITH with_dims AS (
    SELECT a.*, p.category
    FROM store_sales a
    INNER JOIN products p
        ON a.product_id = p.id
    WHERE p.launch_date >= '2024-05-01'
)
SELECT
    id,
    store_id,
    sale_date,
    revenue
FROM with_dims
ORDER BY store_id DESC NULLS LAST
