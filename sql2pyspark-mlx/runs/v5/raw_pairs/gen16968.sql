with filtered as (
    -- Keep recent, valid rows
    select
        id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee * discount as shipping_fee_times_discount,
        trim(status) as clean_status
    from orders
    where channel is distinct from 'web'
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from filtered src
    left join promotions pr
        on src.channel = pr.channel
       and src.order_date >= pr.starts_on
       and src.order_date < pr.ends_on + interval '30' day
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        status,
        discount,
        promo_id,
        discount_pct,
        row_number() over (order by amount, id, promo_id) as event_seq,
        lag(promo_id, 1) over (order by order_date, id, promo_id) as prev_promo_id,
        lag(promo_id, 1) over (partition by promo_code order by discount, id, promo_id) as prev_promo_id_2,
        ntile(3) over (partition by promo_code order by discount desc, id, promo_id) as discount_bucket,
        corr(amount, discount) over (partition by promo_code) as corr_amount_discount
    from with_window
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by status order by prev_promo_id desc, id, promo_id) as rn
    from scored
    qualify rn = 1
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from deduped s
    where exists (
            select 1
            from shipments x
            where x.order_id = s.id
              and x.delivered_date between '2023-03-01' and '2024-10-01'
        )
)
-- Multi-level aggregation
select
    case when discount >= 0.41 then 'HIGH' else 'LOW' end as discount_tier,
    count_if(status = 'COMPLETED') as completed_events,
    stddev_samp(discount) as discount_stddev,
    avg(discount_bucket) filter (where status = 'CANCELLED') as cancelled_avg_discount_bucket
from eligible
group by discount_tier
order by discount_tier nulls last
