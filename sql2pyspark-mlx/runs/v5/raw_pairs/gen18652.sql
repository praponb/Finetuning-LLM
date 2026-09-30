SELECT product_id, COUNT(*) AS row_count FROM inventory WHERE quantity BETWEEN 107 AND 211 AND last_restock <= '2025-08-06' GROUP BY product_id ORDER BY row_count DESC, product_id ASC LIMIT 50;
