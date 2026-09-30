select order_id, product_id, round(unit_price, 2) as unit_price_rounded from order_items where order_id in (5, 24) order by unit_price_rounded desc;
