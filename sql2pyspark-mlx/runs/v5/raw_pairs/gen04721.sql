SELECT *, COALESCE(revenue, 0) AS revenue_filled FROM store_sales WHERE store_id = 1 AND sale_date BETWEEN '2024-07-29' AND '2025-03-01' AND units != 31 ORDER BY id DESC
