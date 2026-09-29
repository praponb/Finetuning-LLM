select id, order_id, quantity, product_id from (select *, rank() over (partition by order_id order by quantity desc) as row_num from order_items) as r where row_num <= 1 order by id desc
