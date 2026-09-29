SELECT
    QUARTER(last_restock) AS last_restock_quarter,
    MAX(quantity) - MIN(quantity) AS quantity_range,
    ROUND(STDDEV(quantity), 2) AS stddev_quantity,
    SUM(quantity) AS total_quantity
FROM inventory
WHERE NOT ((product_id = 2 AND quantity < 226) OR store_id IN (1, 4))
GROUP BY last_restock_quarter;
