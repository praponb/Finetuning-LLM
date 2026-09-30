SELECT p.brand, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue, SUM(ss.revenue) AS total_revenue
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
WHERE ss.revenue BETWEEN 1100 AND 2600
GROUP BY p.brand
HAVING ROUND(STDDEV(ss.revenue), 2) > 23
ORDER BY stddev_revenue DESC;
