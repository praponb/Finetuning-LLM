select * from (select id, order_id, unit_price, product_id, row_number() over (partition by order_id order by unit_price desc, id) row_num from order_items) r where row_num <= 5
