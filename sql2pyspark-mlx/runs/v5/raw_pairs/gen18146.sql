select s.id, s.supplier_name, t.stddev_cost
from suppliers s
inner join (select supplier_id, round(stddev(cost), 2) as stddev_cost
    from products
    group by supplier_id) t on s.id = t.supplier_id
where t.stddev_cost >= 29;
