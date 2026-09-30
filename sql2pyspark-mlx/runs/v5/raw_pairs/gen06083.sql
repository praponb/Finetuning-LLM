SELECT *
FROM (
    SELECT product_id, store_id, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    WHERE product_id = 4
    GROUP BY product_id, store_id
) AS sub
WHERE stddev_units < 71
ORDER BY store_id DESC, product_id
