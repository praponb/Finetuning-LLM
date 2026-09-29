select unit_price, order_id, greatest(quantity, unit_price) as greatest_quantity_unit_price from order_items where unit_price < 140 order by order_id desc
