SELECT products.brand, SUM(store_sales.revenue) AS total_revenue
FROM store_sales
RIGHT OUTER JOIN products ON store_sales.product_id = products.id
INNER JOIN stores ON store_sales.store_id = stores.id
GROUP BY products.brand
ORDER BY total_revenue DESC, products.brand;
