select id, order_id, quantity, min(quantity) over (partition by order_id) order_id_min_quantity from order_items
