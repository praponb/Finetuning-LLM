with base as (
    select *, case when channel = 'web' then 1 else 0 end as is_web
    from orders
    where order_date >= date '2023-09-17'
      and store_id is not null
)
select
    s.id,
    s.order_date,
    s.status,
    s.channel,
    s.discount,
    s.shipping_fee,
    x.delivered_date
from base s
join lateral (
        select c.delivered_date
        from shipments c
        where c.order_id = s.id
          and c.shipped_date <= s.order_date
        order by c.shipped_date desc, c.id desc
        limit 1
    ) x on true;
