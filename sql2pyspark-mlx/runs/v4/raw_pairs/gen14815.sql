SELECT src.*, d.discount FROM payments src LEFT JOIN orders d ON src.order_id = d.id
