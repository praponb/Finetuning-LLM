SELECT
    ss.id,
    ss.sale_date,
    ss.store_id,
    ss.units
FROM store_sales ss
WHERE YEAR(ss.sale_date) = 2023 AND ss.product_id NOT IN (SELECT id FROM products WHERE category <> 'Fashion');
