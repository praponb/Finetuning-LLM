SELECT status, SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END) shipped_count, SUM(shipping_fee) total_shipping_fee FROM orders GROUP BY status ORDER BY total_shipping_fee DESC;
