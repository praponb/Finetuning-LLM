SELECT
    id,
    store_id,
    units,
    LEAD(units) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_units,
    LAG(units) OVER (PARTITION BY store_id ORDER BY revenue, id) AS prev_units,
    LAG(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id) AS prev_revenue,
    LEAD(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_revenue,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS event_seq,
    SUM(units) OVER (PARTITION BY product_id) AS product_id_sum_units
FROM store_sales;
