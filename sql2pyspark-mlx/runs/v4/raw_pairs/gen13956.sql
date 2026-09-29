select s.city, count(distinct p.amount) unique_statuss from payments p left join orders o on p.order_id = o.id right join stores s on o.store_id = s.id group by s.city
