with filtered as (
    select
        *,
        substring(channel, 1, 4) as channel_prefix,
        coalesce(shipping_fee, 0) as shipping_fee_filled,
        coalesce(channel, 'NONE') as resolved_channel
    from orders
    where customer_id is not null
      and order_date >= to_date('2023-07-19')
),
latest as (
    select *, rank() over (partition by customer_id order by amount desc nulls last) as dedup_rank
    from filtered
    qualify dedup_rank = 1
),
scored as (
    select
        *,
        dense_rank() over (partition by store_id order by shipping_fee desc nulls first) as shipping_fee_dense_rank,
        corr(discount, shipping_fee) over (partition by store_id) as corr_discount_shipping_fee
    from latest
),
metrics as (
    select
        id,
        customer_id,
        order_date,
        amount,
        shipping_fee_filled,
        resolved_channel,
        shipping_fee_dense_rank,
        sum(amount) over w_grp as last4_sum_amount,
        min(amount) over w_grp as last4_min_amount,
        max(amount) over w_grp as last4_max_amount,
        count(amount) over w_grp as last4_count_amount
    from scored
    window w_grp as (partition by status, store_id order by amount, id rows between 3 preceding and current row)
)
select
    d.segment,
    count(*) as cnt,
    round(avg(src.last4_max_amount), 2) as avg_last4_max_amount,
    round(avg(src.shipping_fee_filled), 2) as avg_shipping_fee_filled
from metrics src
left join customers d on src.customer_id = d.id
group by rollup (d.segment);
