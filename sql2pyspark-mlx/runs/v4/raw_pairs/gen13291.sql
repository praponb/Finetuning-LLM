select o.id, o.channel, s.store_name, s.region from orders o inner join stores s on s.id = o.store_id;
