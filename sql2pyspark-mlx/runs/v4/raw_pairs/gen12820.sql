with valid as (
    -- Filter the raw rows and derive helper columns
    select *, case when channel = 'web' then 1 else 0 end as is_web
    from orders
    where order_date > to_date('2023-04-10')
      and customer_id is not null
      and shipping_fee is not null
)
-- Keep one row per key (latest first)
select *
from (
    select *, row_number() over (partition by store_id order by discount desc, id) as rn
    from valid
) t
where rn = 1
