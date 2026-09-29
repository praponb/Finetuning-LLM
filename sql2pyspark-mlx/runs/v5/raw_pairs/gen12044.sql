with store_summary as (
    select store_id, min(sale_date) as min_sale_date
    from store_sales
    group by store_id
)
select
    s.id,
    s.sqft,
    s.city,
    ss.min_sale_date
from stores s
left join store_summary ss on s.id = ss.store_id;
