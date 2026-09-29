SELECT store_id, units FROM store_sales WHERE sale_date BETWEEN '2023-05-01' AND '2025-09-01' UNION ALL SELECT store_id, units FROM store_sales WHERE units BETWEEN 12 AND 17 ORDER BY units DESC;
