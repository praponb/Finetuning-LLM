SELECT id, quantity, NTILE(10) OVER (ORDER BY quantity DESC, id) AS quartile FROM inventory
