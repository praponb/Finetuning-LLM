select
    dim.region,
    count(*) as cnt,
    avg(e.units) as avg_units,
    min(e.revenue) as min_revenue
from store_sales e
inner join stores dim on e.store_id = dim.id
group by dim.region;
