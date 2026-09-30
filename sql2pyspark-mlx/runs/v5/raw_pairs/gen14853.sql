SELECT
    id,
    store_id,
    discount,
    order_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY discount DESC, id) AS rnk
    FROM orders
) AS r
WHERE rnk = 1
ORDER BY order_date
