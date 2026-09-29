select o.status, concat_ws(', ', sort_array(collect_set(p.status))) as status_values from payments p right join orders o on o.id = p.order_id group by o.status order by status_values, status limit 5;
