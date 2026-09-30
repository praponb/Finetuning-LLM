with store_summary as (
    select store_id, min(revenue) min_revenue
    from store_sales
    group by store_id
)
select s.id, s.city, ss.min_revenue
from stores s
inner join store_summary ss on s.id = ss.store_id
order by s.city desc, id desc
limit 5
