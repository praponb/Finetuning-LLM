SELECT o.id, o.discount FROM orders o WHERE o.store_id IN (SELECT id FROM stores WHERE region IN ('East', 'South', 'West')) ORDER BY o.discount DESC;
