SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    revenue,
    RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS FIRST) AS revenue_rank,
    NTILE(10) OVER (PARTITION BY store_id ORDER BY revenue, id) AS revenue_bucket,
    ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS event_seq
FROM store_sales;
