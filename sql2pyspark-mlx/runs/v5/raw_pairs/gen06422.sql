select
    d.country,
    e.category,
    count(*) as cnt,
    min(e.cost) as min_cost,
    grouping_id() as grouping_level
from products e
join suppliers d on e.supplier_id = d.id
group by cube (d.country, e.category)
order by cnt nulls first
