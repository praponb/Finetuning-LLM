with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        upper(plan) as plan_upper
    from subscriptions
    where end_date < '2025-06-02'
),
enriched as (
    -- Attach customers attributes
    select
        e.*,
        c.name,
        c.city,
        c.age
    from filtered e
    left join customers c
        on e.customer_id = c.id
    where c.email is null
),
enriched_subscriptions as (
    -- Attach customers attributes
    select
        e.*,
        p.signup_date,
        p.segment
    from enriched e
    inner join customers p
        on e.customer_id = p.id
),
remaining_keys as (
    -- Set intersection on keys
    select status, id
    from enriched_subscriptions
    intersect
    select status, id
    from subscriptions
    where year(end_date) >= 2023
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched_subscriptions s
    inner join remaining_keys k on s.status = k.status and s.id = k.id
),
joined as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.plan,
        s.start_date,
        s.plan_upper,
        s.name,
        s.city,
        s.age,
        s.signup_date,
        s.segment,
        p.age as customer_age,
        p.segment as customer_segment
    from verified s
    inner join customers p
        on s.customer_id = p.id
),
with_dims as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.name,
        s.signup_date,
        s.segment,
        s.customer_age,
        d.segment as customer_segment_2,
        d.city as customer_city
    from joined s
    left join customers d
        on s.customer_id <=> d.id
),
with_dims_subscriptions as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.name,
        e.signup_date,
        e.segment,
        e.customer_segment_2,
        b.email,
        b.city
    from with_dims e
    left join customers b
        on e.customer_id = b.id
),
daily as (
    -- Grouped metrics
    select
        segment,
        count(case when customer_segment_2 = 'SMB' then 1 end) as smb_count,
        sort_array(collect_set(city)) as city_list
    from with_dims_subscriptions
    group by segment
    having smb_count > 1
)
select segment, city_list
from daily;
