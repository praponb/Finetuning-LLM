WITH enriched AS (
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS quantity_dense_rank
    FROM inventory
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.quantity_avg_6_prior,
    s.quantity_dense_rank,
    (SELECT AVG(s2.revenue) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS avg_store_sale_revenue
FROM enriched s;
