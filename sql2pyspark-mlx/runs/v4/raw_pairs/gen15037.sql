SELECT MONTH(s.opened_date) AS opened_month, CASE WHEN s.sqft >= 17900 THEN 'high' WHEN s.sqft >= 5151 THEN 'mid' ELSE 'low' END AS sqft_bucket, SUM(i.quantity) AS total_quantity
FROM inventory i
FULL OUTER JOIN products p ON p.id = i.product_id
RIGHT JOIN stores s ON i.store_id = s.id
GROUP BY MONTH(s.opened_date), CASE WHEN s.sqft >= 17900 THEN 'high' WHEN s.sqft >= 5151 THEN 'mid' ELSE 'low' END;
