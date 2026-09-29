select s.city, count(distinct o.amount) as unique_statuss, round(stddev(o.discount), 2) as stddev_discount from orders o inner join stores s on o.store_id = s.id group by s.city;
