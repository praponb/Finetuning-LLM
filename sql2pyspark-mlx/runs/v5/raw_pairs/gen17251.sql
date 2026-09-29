SELECT id, units, revenue, store_id, FLOOR(units) AS units_whole FROM store_sales WHERE sale_date BETWEEN '2024-03-26' AND '2024-11-18' ORDER BY revenue
