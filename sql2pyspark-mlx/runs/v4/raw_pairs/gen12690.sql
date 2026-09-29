with ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by status order by order_date asc) as row_num
        from orders
    ) t
    where row_num = 1
)
select
    s.id,
    s.customer_id,
    s.status,
    s.channel,
    s.amount,
    s.discount,
    s.shipping_fee,
    s.row_num,
    case when channel rlike '^(web|partner)$' then 'GROUP_A' when channel rlike '^(mobile|store)$' then 'CORE' else 'PRIORITY' end as channel_zone,
    (select min(t2.shipping_fee) from orders t2 where t2.customer_id = s.customer_id) as min_order_shipping_fee
from ranked s
order by row_num asc, id asc
