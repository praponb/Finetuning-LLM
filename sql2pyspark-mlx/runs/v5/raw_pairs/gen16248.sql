SELECT
    id,
    store_id,
    sale_date,
    product_id,
    units,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) AS sale_date_row,
    LAST_VALUE(sale_date) OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sale_date
FROM store_sales
