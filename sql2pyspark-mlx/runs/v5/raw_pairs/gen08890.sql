SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) row_num
    FROM store_sales
) r
WHERE row_num <= 1;
