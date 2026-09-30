select order_id, max(unit_price) as max_unit_price, count(*) as row_count from order_items group by order_id order by max_unit_price;
