select s.city, sum(case when p.brand = 'Globex' then 1 else 0 end) globex_count, round(stddev(p.cost), 2) stddev_cost
from products p
inner join suppliers s on p.supplier_id = s.id
where s.rating between 2.2 and 2.9
group by s.city;
