with kept as (
    select s.*
    from stores s
    where not exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date < s.opened_date
        )
)
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft
from kept
