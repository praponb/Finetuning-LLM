SELECT oi.id, oi.quantity, oi.unit_price, p.cost, p.category, s.rating FROM order_items oi JOIN products p ON oi.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id
