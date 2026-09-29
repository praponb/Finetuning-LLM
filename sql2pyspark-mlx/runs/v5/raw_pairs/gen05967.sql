SELECT s.id, s.city, s.region FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.status IN ('COMPLETED', 'SHIPPED', 'CANCELLED')) ORDER BY s.region;
