with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        substring(device, 1, 3) as device_prefix,
        coalesce(country, 'N/A') as country_filled
    from sessions
    where country is not distinct from 'JP'
      and hour(started_at) between 9 and 15
),
remaining_keys as (
    -- Set intersection on keys
    select customer_id, country
    from filtered
    intersect
    select customer_id, country
    from sessions
    where customer_id is not null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join remaining_keys k on s.customer_id = k.customer_id and s.country = k.country
),
with_dims as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.device,
        src.started_at,
        src.country,
        src.device_prefix,
        c.credit_limit
    from verified src
    left join customers c
        on src.customer_id = c.id
),
joined as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.device,
        s.device_prefix,
        c.email,
        c.segment
    from with_dims s
    left join customers c
        on s.customer_id = c.id
)
-- Multi-level aggregation
select
    customer_id,
    device,
    count_if(segment = 'Corporate') as corporate_events,
    count_if(segment = 'Consumer') as consumer_events,
    count(*) as cnt
from joined
group by customer_id, device;
