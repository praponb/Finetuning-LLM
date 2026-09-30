SELECT COUNT(*) AS cnt, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM inventory
WHERE last_restock >= '2023-10-02';
