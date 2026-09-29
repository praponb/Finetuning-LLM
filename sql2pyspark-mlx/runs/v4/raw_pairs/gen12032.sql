SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY order_id, product_id ORDER BY quantity NULLS FIRST) AS rn
    FROM order_items
) ranked
WHERE rn = 1
