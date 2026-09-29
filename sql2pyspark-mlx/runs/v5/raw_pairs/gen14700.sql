select id, product_id, quantity, unit_price from (select *, row_number() over (partition by product_id order by quantity desc, id) as rank_in_group from order_items) t where rank_in_group = 1
