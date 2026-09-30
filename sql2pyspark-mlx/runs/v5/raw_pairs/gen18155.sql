SELECT
    suppliers.city,
    CASE WHEN suppliers.rating >= 4.5 THEN 'high' WHEN suppliers.rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket,
    MIN(store_sales.units) AS min_units,
    MIN(store_sales.revenue) AS min_revenue
FROM store_sales
INNER JOIN products ON store_sales.product_id = products.id
LEFT OUTER JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY suppliers.city, CASE WHEN suppliers.rating >= 4.5 THEN 'high' WHEN suppliers.rating >= 2.0 THEN 'mid' ELSE 'low' END
HAVING MIN(store_sales.revenue) >= 3500
