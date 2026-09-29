select src.id, src.product_id, src.quantity, src.unit_price, b.product_name, b.launch_date from order_items src left join products b on src.product_id = b.id where b.cost >= 340;
