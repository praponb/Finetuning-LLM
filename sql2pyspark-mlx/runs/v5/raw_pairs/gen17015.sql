SELECT *
FROM (
    SELECT product_id, store_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM inventory
    WHERE quantity >= 208
    GROUP BY product_id, store_id
) AS agg
WHERE stddev_quantity < 40
