select id, amount, store_id, first_value(store_id) over (order by amount, id) as first_store_id from orders
