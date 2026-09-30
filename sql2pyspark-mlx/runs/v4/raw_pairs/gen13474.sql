SELECT
    id,
    customer_id,
    order_date,
    discount
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS row_num
    FROM orders
) AS r
WHERE row_num <= 1
ORDER BY discount;
