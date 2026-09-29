select
    dim.category,
    count(*) as cnt,
    min(e.units) as min_units
from store_sales e
inner join products dim on e.product_id = dim.id
group by dim.category;
