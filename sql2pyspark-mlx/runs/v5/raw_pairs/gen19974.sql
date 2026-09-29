with products_by_supplier as (
    select supplier_id, round((100 * sum(case when category = 'Fashion' then 1 else 0 end)) / count(*), 1) as pct_fashion, sum(case when brand = 'Umbrella' then 1 else 0 end) as umbrella_count
    from products
    where cost < 240
    group by supplier_id
)
select
    suppliers.supplier_name,
    suppliers.rating,
    products_by_supplier.pct_fashion,
    products_by_supplier.umbrella_count
from suppliers
inner join products_by_supplier on suppliers.id = products_by_supplier.supplier_id
order by pct_fashion, supplier_name;
