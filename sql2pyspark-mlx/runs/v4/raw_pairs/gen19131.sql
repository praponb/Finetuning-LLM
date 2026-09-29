select id, unit_price, round(quantity * 0.9, 2) as quantity_scaled from order_items order by unit_price, id limit 20
