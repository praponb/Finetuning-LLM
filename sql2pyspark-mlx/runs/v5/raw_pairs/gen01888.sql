with enriched as (
    -- Ranking and gap features
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        avg(cost) over (partition by carrier, order_id order by cost, id rows between 5 preceding and 1 preceding) as cost_avg_5_prior,
        sum(cost) over w_running as running_cost,
        row_number() over (order by shipped_date, id) as event_seq
    from shipments
    where status = 'LOST'
    window w_running as (partition by status order by cost, id rows between unbounded preceding and current row)
),
joined as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.running_cost,
        p.status,
        p.amount,
        p.channel
    from enriched e
    left join orders p
        on e.order_id = p.id
),
base as (
    -- Keep recent, valid rows
    select
        id,
        running_cost,
        status,
        amount,
        channel,
        amount + running_cost as amount_plus_running_cost,
        coalesce(status, 'NONE') as resolved_status
    from joined
    where status is not distinct from 'SHIPPED'
      and amount > 1120
),
base_shipments as (
    -- Basic cleansing and defaults
    select
        *,
        running_cost - amount as running_cost_minus_amount,
        coalesce(status, 'OTHER') as status_filled
    from base
    where amount <= 930
)
-- Multi-level aggregation
select
    channel,
    case when running_cost >= 518 then 'PREMIUM' else 'STANDARD' end as running_cost_tier,
    approx_percentile(amount_plus_running_cost, 0.9) as amount_plus_running_cost_p90,
    count(case when status = 'CANCELLED' then 1 end) as cancelled_count,
    round(sum(amount_plus_running_cost) / nullif(count(case when status = 'SHIPPED' then 1 end), 0), 2) as amount_plus_running_cost_per_shipped
from base_shipments
group by cube (channel, case when running_cost >= 518 then 'PREMIUM' else 'STANDARD' end)
