SELECT id, sale_date, units, ROUND(AVG(units) OVER (ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS units_moving_avg_7 FROM store_sales
