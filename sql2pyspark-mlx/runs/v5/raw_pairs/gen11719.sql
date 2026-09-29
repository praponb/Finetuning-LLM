select channel_group, shipping_fee_bucket, amount_range
from (
    select case when channel in ('mobile', 'partner') then 'group_a' else 'other' end as channel_group, case when shipping_fee >= 38 then 'high' when shipping_fee >= 12 then 'mid' else 'low' end as shipping_fee_bucket, max(amount) - min(amount) as amount_range
    from orders
    group by case when channel in ('mobile', 'partner') then 'group_a' else 'other' end, case when shipping_fee >= 38 then 'high' when shipping_fee >= 12 then 'mid' else 'low' end
) as t
where amount_range > 42
order by channel_group, shipping_fee_bucket
limit 20
