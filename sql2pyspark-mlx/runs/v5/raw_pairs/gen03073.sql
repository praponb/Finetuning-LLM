with cleaned as (
    select *, trim(status) as clean_status
    from orders
    where order_date >= date '2023-06-13'
)
select
    date_trunc('month', order_date) as activity_month,
    count(distinct customer_id) as distinct_customer_ids,
    size(collect_set(channel)) as num_channels,
    avg(discount) filter (where channel = 'partner') as partner_avg_discount,
    count(case when channel = 'store' then 1 end) as store_count
from cleaned
group by date_trunc('month', order_date)
order by distinct_customer_ids desc nulls first
