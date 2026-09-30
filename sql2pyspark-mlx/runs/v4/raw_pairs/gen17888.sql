with deduped as (
    -- Keep one row per key (latest first)
    select *
    from payments
    qualify rank() over (partition by order_id order by paid_at nulls last) = 1
),
with_dims as (
    -- Attach orders attributes
    select
        e.*,
        p.shipping_fee,
        p.discount,
        p.channel
    from deduped e
    inner join orders p
        on e.order_id = p.id
),
deduped_payments as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by method, status order by shipping_fee desc nulls first, id) as latest_rank
    from with_dims
    qualify latest_rank = 1
)
-- Multi-level aggregation
select
    case when shipping_fee >= 32 then 'PREMIUM' else 'STANDARD' end as shipping_fee_tier,
    sort_array(collect_set(status)) as status_list,
    count(case when channel = 'partner' then 1 end) as partner_count,
    count(case when status = 'REFUNDED' then 1 end) as refunded_count,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    avg(amount) as avg_amount
from deduped_payments
group by cube (case when shipping_fee >= 32 then 'PREMIUM' else 'STANDARD' end)
order by shipping_fee_tier desc
