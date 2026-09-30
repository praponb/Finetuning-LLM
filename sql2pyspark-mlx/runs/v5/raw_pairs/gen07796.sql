SELECT *
FROM (
    SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    WHERE unit_price <= 230
    GROUP BY product_id
) AS agg
WHERE stddev_quantity < 52;
