select s.region, sum(o.discount) as total_discount, count(*) as num_orders from orders o join stores s on s.id = o.store_id group by s.region;
