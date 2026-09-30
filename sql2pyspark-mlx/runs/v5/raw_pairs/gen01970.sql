SELECT
    id,
    supplier_id,
    launch_date,
    cost,
    SUM(cost) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
FROM products
WHERE price BETWEEN 770 AND 820
ORDER BY id DESC, 4;
