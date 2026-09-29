with enriched as (
    select
        a.id,
        a.order_id,
        a.carrier,
        a.shipped_date,
        a.delivered_date,
        a.cost,
        d.order_date,
        d.shipping_fee,
        d.channel
    from shipments a
    left join orders d
        on a.order_id = d.id
    where d.customer_id = 13
),
with_windows as (
    select
        *,
        row_number() over (partition by order_id, carrier order by cost desc, id) as event_seq,
        sum(shipping_fee) over (partition by carrier, order_id order by shipped_date, id rows between unbounded preceding and current row) as running_shipping_fee,
        avg(cost) over (order by cost, id rows between 3 preceding and 1 preceding) as cost_avg_3_prior
    from enriched
),
attributed as (
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from with_windows src
    inner join promotions pr
        on coalesce(src.channel, 'web') = pr.channel
       and src.shipped_date between pr.starts_on and pr.ends_on
),
with_windows_shipments as (
    select
        id,
        order_id,
        order_date,
        channel,
        running_shipping_fee,
        promo_id,
        promo_code,
        sum(running_shipping_fee) over (partition by carrier order by cost_avg_3_prior, id, promo_id rows between unbounded preceding and current row) as cumulative_running_shipping_fee,
        rank() over w_running as event_seq_rank
    from attributed
    window w_running as (partition by channel order by event_seq nulls first)
)
select
    case when event_seq_rank >= 8 then 'PREMIUM' else 'STANDARD' end as event_seq_rank_tier,
    date_trunc('year', order_date) as year_start,
    sum(cumulative_running_shipping_fee) as total_cumulative_running_shipping_fee,
    sum(case when promo_code = 'VIP' then running_shipping_fee else 0 end) as vip_running_shipping_fee,
    count(*) as cnt,
    round(avg(event_seq_rank), 2) as avg_event_seq_rank,
    size(collect_set(channel)) as num_channels
from with_windows_shipments
group by cube (case when event_seq_rank >= 8 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', order_date))
having cnt > 1;
