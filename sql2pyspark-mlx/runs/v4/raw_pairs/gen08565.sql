SELECT store_id, max_quantity
FROM (
    SELECT store_id, MAX(quantity) AS max_quantity
    FROM inventory
    GROUP BY store_id
) AS t
WHERE max_quantity < 240;
