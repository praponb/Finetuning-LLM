with product_totals as (
    select product_id, max(sale_date) as latest_sale_date, max(units) as max_units
    from store_sales
    group by product_id
)
select
    p.id,
    p.category,
    pt.latest_sale_date,
    pt.max_units
from products p
inner join product_totals pt on p.id = pt.product_id
where pt.max_units >= 18
order by pt.latest_sale_date
