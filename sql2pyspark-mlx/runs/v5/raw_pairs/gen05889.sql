SELECT
    id,
    supplier_id,
    brand,
    launch_date,
    price,
    category,
    cost,
    LEAD(price, 1) OVER (PARTITION BY supplier_id, brand ORDER BY launch_date, id) AS next_price,
    RANK() OVER (PARTITION BY category ORDER BY cost DESC) AS cost_rank
FROM products
WHERE YEAR(launch_date) >= 2025;
