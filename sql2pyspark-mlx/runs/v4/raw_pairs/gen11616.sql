SELECT store_id, SUM(discount) AS total_discount FROM orders WHERE order_date >= '2023-09-01' GROUP BY 1 ORDER BY store_id DESC, total_discount DESC;
