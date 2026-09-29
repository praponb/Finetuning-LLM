with product_totals as (
    select product_id, max(sale_date) as last_sale_date, count(units) as units_count
    from store_sales
    where product_id = 6
    group by product_id
)
select
    p.id,
    p.price,
    p.product_name,
    pt.last_sale_date,
    pt.units_count
from products p
join product_totals pt on p.id = pt.product_id
where pt.units_count >= 5
order by price desc, p.product_name asc
