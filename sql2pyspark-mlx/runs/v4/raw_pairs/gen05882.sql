SELECT id, product_id, revenue
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) pos
    FROM store_sales
) t
WHERE pos <= 2;
