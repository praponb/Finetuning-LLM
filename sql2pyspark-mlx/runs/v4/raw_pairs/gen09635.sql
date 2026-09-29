SELECT
    id,
    supplier_id,
    launch_date,
    price,
    LEAD(price, 1) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS next_price,
    PERCENT_RANK() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC) AS launch_date_pct_rank
FROM products;
