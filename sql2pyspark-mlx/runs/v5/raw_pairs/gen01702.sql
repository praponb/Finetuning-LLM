SELECT
    id,
    store_id,
    revenue,
    RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) AS revenue_rank
FROM store_sales
