SELECT c.is_active, MIN(o.shipping_fee) AS min_shipping_fee FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE c.segment IN ('SMB', 'Corporate', 'Consumer') GROUP BY c.is_active
