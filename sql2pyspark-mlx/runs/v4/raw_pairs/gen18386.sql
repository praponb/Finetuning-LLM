select o.channel, concat_ws(', ', sort_array(collect_set(s.carrier))) as carrier_values from shipments s join orders o on s.order_id = o.id where s.order_id = 36 group by o.channel order by channel;
