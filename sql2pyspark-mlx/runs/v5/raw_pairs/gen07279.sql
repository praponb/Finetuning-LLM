SELECT id, delivered_date FROM shipments WHERE status IN ('DELIVERED', 'RETURNED') EXCEPT SELECT id, delivered_date FROM shipments WHERE delivered_date <= '2024-01-02' ORDER BY delivered_date
