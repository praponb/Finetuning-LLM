with product_totals as (
    select product_id, round(stddev(units), 2) as stddev_units
    from store_sales
    group by product_id
)
select p.id, p.price, pt.stddev_units
from products p
inner join product_totals pt on p.id = pt.product_id
order by stddev_units desc
