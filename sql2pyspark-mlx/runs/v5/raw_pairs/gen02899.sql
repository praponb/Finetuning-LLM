select s.* from orders s where exists (select 1 from order_items x where x.order_id = s.id and x.order_id in (19, 34))
