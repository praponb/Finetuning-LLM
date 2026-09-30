SELECT
    ss.product_id,
    p.stock_qty,
    p.price,
    DATEDIFF(CURRENT_DATE(), ss.sale_date) AS days_since_sale_date
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
WHERE LENGTH(p.product_name) < 10 AND p.category = 'Fashion';
