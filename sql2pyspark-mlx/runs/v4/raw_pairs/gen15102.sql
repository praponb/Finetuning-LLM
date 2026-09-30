SELECT CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END AS unit_price_bucket, SUM(unit_price) AS total_unit_price, SUM(SUM(unit_price)) OVER (ORDER BY CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END) AS cumulative_unit_price
FROM order_items
GROUP BY CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END
ORDER BY total_unit_price DESC, cumulative_unit_price DESC
