with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(carrier, 'Unknown') as carrier_filled,
        coalesce(carrier, 'UNKNOWN') as carrier_clean
    from shipments
    where carrier = 'Flash'
      and carrier is not distinct from 'DHL'
),
joined as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.cost,
        b.channel
    from filtered src
    left join orders b
        on src.order_id = b.id
),
base as (
    -- Basic cleansing and defaults
    select *, initcap(trim(channel)) as channel_title
    from joined
    where order_id is not null
      and channel is not distinct from 'partner'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        first_value(cost) ignore nulls over (partition by order_id order by cost, id rows between unbounded preceding and current row) as first_known_cost,
        rank() over (order by cost desc nulls last) as cost_rank,
        lead(cost, 2) over (partition by channel order by cost, id) as next_cost,
        dense_rank() over (partition by order_id, channel order by cost desc nulls first) as cost_dense_rank,
        row_number() over (order by cost desc, id) as row_seq
    from base
    where cost <= 44
)
-- Native SQL PIVOT
select *
from (
    select
        order_id,
        channel,
        cost_dense_rank
    from enriched
) src
pivot (
    max(cost_dense_rank) as total,
    count(1) as cnt
    for channel in ('store', 'web')
)
