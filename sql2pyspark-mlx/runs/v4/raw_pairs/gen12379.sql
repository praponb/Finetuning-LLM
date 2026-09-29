select order_id, avg(quantity) as avg_quantity, count(unit_price) as unit_price_count from order_items where unit_price <= 120 group by 1 order by unit_price_count desc, order_id limit 5
