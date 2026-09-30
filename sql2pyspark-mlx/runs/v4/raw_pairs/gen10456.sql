with with_latest as (
    select s.*, latest.sale_date
    from stores s
    left join lateral (
            select c.sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    store_name,
    city,
    opened_date,
    sqft
from with_latest
order by city desc nulls last
