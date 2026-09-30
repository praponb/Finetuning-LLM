select *, round(unit_price, 1) as unit_price_rounded, quantity - unit_price as line_total from order_items where product_id in (7, 16, 38, 39);
