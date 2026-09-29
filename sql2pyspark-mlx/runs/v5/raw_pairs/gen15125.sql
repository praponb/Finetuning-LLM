with scored as (
    select
        id,
        customer_id,
        store_id,
        channel,
        amount,
        avg(discount) over (partition by customer_id, channel order by order_date asc, id asc rows between 3 preceding and 1 preceding) as discount_avg_3_prior,
        row_number() over (order by order_date desc, id asc) as event_seq,
        first_value(channel, true) over w_grp as first_known_channel
    from orders
    window w_grp as (partition by channel order by order_date asc, id asc rows between unbounded preceding and current row)
)
select
    s.id,
    s.store_id,
    s.event_seq,
    (select min(t2.revenue) from store_sales t2 where t2.store_id = s.store_id) as min_store_sale_revenue
from scored s
order by store_id desc, event_seq asc;
