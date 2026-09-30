with deduped as (
    select *
    from orders
    qualify row_number() over (partition by store_id, customer_id order by order_date desc, id) = 1
)
select
    case when shipping_fee >= 34 then 'LARGE' else 'SMALL' end as shipping_fee_tier,
    sort_array(collect_set(status)) as status_list,
    count(case when status = 'CANCELLED' then 1 end) as cancelled_count,
    sum(shipping_fee) filter (where status = 'RETURNED') as returned_total_shipping_fee,
    sort_array(collect_set(channel)) as channel_list,
    stddev_samp(amount) as amount_stddev,
    count(*) filter (where status = 'COMPLETED') as completed_rows
from deduped
group by case when shipping_fee >= 34 then 'LARGE' else 'SMALL' end
having cancelled_count >= 1
order by returned_total_shipping_fee;
