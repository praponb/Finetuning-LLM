SELECT products.brand, COUNT(store_sales.revenue) revenue_count
FROM store_sales
FULL OUTER JOIN products ON store_sales.product_id = products.id
JOIN suppliers ON products.supplier_id = suppliers.id
GROUP BY products.brand;
