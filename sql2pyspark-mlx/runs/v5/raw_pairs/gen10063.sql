SELECT
    ss.id,
    ss.units,
    ss.store_id,
    p.launch_date,
    p.product_name
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id;
