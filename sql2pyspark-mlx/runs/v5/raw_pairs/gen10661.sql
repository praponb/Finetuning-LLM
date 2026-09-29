select c.is_active, sort_array(collect_set(o.channel)) as channel_list from orders o right outer join customers c on o.customer_id = c.id group by c.is_active
