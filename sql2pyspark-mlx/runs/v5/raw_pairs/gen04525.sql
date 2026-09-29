SELECT
    ss.id,
    ss.units,
    s.region,
    s.opened_date,
    p.cost
FROM store_sales ss
RIGHT JOIN stores s ON s.id = ss.store_id
LEFT JOIN products p ON ss.product_id = p.id
WHERE p.stock_qty >= 298
ORDER BY 5, region
