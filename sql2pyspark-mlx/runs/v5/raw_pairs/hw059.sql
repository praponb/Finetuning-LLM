SELECT id, COALESCE(discount, 0) AS final_discount FROM orders;
