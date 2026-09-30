select
    s.rating,
    s.supplier_name,
    t.min_launch_date,
    t.total_price
from suppliers s
join (select supplier_id, min(launch_date) as min_launch_date, sum(price) as total_price
    from products
    group by supplier_id) t on s.id = t.supplier_id
where t.total_price > 1800
