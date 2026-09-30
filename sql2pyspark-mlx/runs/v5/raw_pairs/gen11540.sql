SELECT
    id,
    store_id,
    revenue,
    sale_date,
    units,
    FIRST_VALUE(sale_date) OVER (PARTITION BY store_id ORDER BY revenue, id) AS first_sale_date,
    LAST_VALUE(units) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_units
FROM store_sales
ORDER BY first_sale_date;
