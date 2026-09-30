SELECT *
FROM (
    SELECT product_id, MAX(quantity) max_quantity
    FROM inventory
    GROUP BY product_id
) sub
WHERE max_quantity < 200;
