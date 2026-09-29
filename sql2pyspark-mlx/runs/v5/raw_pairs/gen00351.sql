select
    s.id,
    s.store_name,
    count(ss.id) as store_sale_count,
    round(avg(ss.units), 2) as avg_units
from stores s
left join store_sales ss on s.id = ss.store_id
group by s.id, s.store_name
having store_sale_count >= 1
order by avg_units, id, store_name
limit 3
