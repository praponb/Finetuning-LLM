with products_by_supplier as (
    select supplier_id, max(price) as max_price, min(launch_date) as min_launch_date
    from products
    group by supplier_id
)
select
    s.id,
    s.supplier_name,
    s.city,
    pbs.max_price,
    pbs.min_launch_date
from suppliers s
join products_by_supplier pbs on s.id = pbs.supplier_id
where pbs.max_price > 970
order by city, s.supplier_name;
