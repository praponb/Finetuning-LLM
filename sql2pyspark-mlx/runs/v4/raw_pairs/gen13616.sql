select
    store_id,
    date_trunc('quarter', order_date) as quarter_start,
    date_trunc('month', order_date) as activity_month,
    sort_array(collect_set(channel)) as channel_list,
    round(sum(shipping_fee) / nullif(count(case when status = 'COMPLETED' then 1 end), 0), 2) as shipping_fee_per_completed,
    count(*) filter (where channel = 'store') as store_rows,
    round(avg(discount), 2) as avg_discount,
    sort_array(collect_set(status)) as status_list,
    count_if(channel = 'web') as web_events
from orders
group by store_id, quarter_start, activity_month
