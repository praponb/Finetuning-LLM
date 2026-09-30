SELECT
    CASE WHEN suppliers.rating >= 4.3 THEN 'high' WHEN suppliers.rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket,
    suppliers.country,
    MAX(store_sales.sale_date) AS latest_sale_date,
    ROUND(AVG(store_sales.units), 2) AS avg_units
FROM store_sales
LEFT JOIN products ON store_sales.product_id = products.id
LEFT JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY CASE WHEN suppliers.rating >= 4.3 THEN 'high' WHEN suppliers.rating >= 1.8 THEN 'mid' ELSE 'low' END, suppliers.country
ORDER BY avg_units DESC;
