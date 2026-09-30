with products_by_supplier as (
    select supplier_id, round(stddev(price), 2) as stddev_price, min(launch_date) as min_launch_date
    from products
    group by supplier_id
)
select s.country, pbs.stddev_price, pbs.min_launch_date
from suppliers s
left outer join products_by_supplier pbs on s.id = pbs.supplier_id
where pbs.stddev_price > 46
