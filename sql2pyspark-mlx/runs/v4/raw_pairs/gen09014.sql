select * from (select id, order_id, unit_price, row_number() over (partition by order_id order by unit_price desc, id) pos from order_items) ranked where pos <= 3;
