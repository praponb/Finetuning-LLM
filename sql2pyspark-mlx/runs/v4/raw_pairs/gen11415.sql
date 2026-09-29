SELECT id, delivered_date, cost FROM shipments WHERE order_id IS NOT NULL AND shipped_date <= '2024-05-01' AND carrier IS NULL;
