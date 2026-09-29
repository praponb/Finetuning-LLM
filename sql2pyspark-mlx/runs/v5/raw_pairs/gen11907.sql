with with_dims as (
    -- Attach customers attributes
    select
        s.*,
        c.segment,
        c.credit_limit
    from sessions s
    left join customers c
        on s.customer_id = c.id
),
remaining_keys as (
    -- Set intersection on keys
    select device
    from with_dims
    intersect all
    select device
    from sessions
    where customer_id is null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    join remaining_keys k on s.device = k.device
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time >= s.started_at
        )
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.session_id in (18, 38)
        )
),
with_dims_sessions as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.segment,
        src.credit_limit,
        p.city
    from kept src
    left join customers p
        on src.customer_id = p.id
),
eligible_sessions as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_dims_sessions s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_type = 'purchase'
        )
),
totals as (
    -- Grouped metrics
    select
        case when credit_limit >= 14800 then 'LARGE' else 'SMALL' end as credit_limit_tier,
        count(*) filter (where city = 'London') as london_rows,
        sort_array(collect_set(city)) as city_list,
        max(credit_limit) filter (where segment = 'SMB') as smb_max_credit_limit,
        grouping(case when credit_limit >= 14800 then 'LARGE' else 'SMALL' end) as grouping_level
    from eligible_sessions
    group by cube (case when credit_limit >= 14800 then 'LARGE' else 'SMALL' end)
)
select
    credit_limit_tier,
    city_list,
    smb_max_credit_limit,
    grouping_level
from totals
order by smb_max_credit_limit desc nulls last;
