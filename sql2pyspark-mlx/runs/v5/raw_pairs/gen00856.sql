SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) row_num
    FROM store_sales
    WHERE units < 23
) r
WHERE row_num <= 3;
