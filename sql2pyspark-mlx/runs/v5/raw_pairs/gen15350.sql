with joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.plan,
        a.end_date,
        a.monthly_fee,
        a.status,
        c.city
    from subscriptions a
    left join customers c
        on a.customer_id = c.id
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        city,
        count(monthly_fee) over (partition by city, plan order by monthly_fee, id rows between unbounded preceding and current row) as running_count_monthly_fee,
        min(monthly_fee) over (partition by city, plan order by monthly_fee, id rows between unbounded preceding and current row) as running_min_monthly_fee,
        max(monthly_fee) over (partition by city, plan order by monthly_fee, id rows between unbounded preceding and current row) as running_max_monthly_fee,
        row_number() over (order by monthly_fee, id) as event_seq
    from joined
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        c.age,
        c.city as customer_city,
        c.credit_limit
    from scored a
    left join customers c
        on a.customer_id = c.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        city,
        running_count_monthly_fee,
        running_min_monthly_fee,
        customer_city,
        credit_limit,
        round(running_count_monthly_fee / event_seq, 2) as running_count_monthly_fee_per_event_seq,
        coalesce(city, 'UNKNOWN') as city_clean
    from enriched
    where city is not distinct from 'Chiang Mai'
      and customer_id in (8, 13)
),
cleaned as (
    -- Keep recent, valid rows
    select *, case when running_count_monthly_fee_per_event_seq >= 88 then 'High' when running_count_monthly_fee_per_event_seq >= 11 then 'Medium' else 'Low' end as running_count_monthly_fee_per_event_seq_level
    from valid
    where city in ('Bangkok', 'New York')
),
joined_subscriptions as (
    -- Attach customers attributes
    select s.*, c.country
    from cleaned s
    inner join customers c
        on s.customer_id = c.id
),
valid_subscriptions as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        city,
        running_count_monthly_fee,
        customer_city,
        credit_limit,
        city_clean,
        running_count_monthly_fee_per_event_seq_level,
        running_count_monthly_fee_per_event_seq * credit_limit as running_count_monthly_fee_per_event_seq_times_credit_limit,
        running_count_monthly_fee - running_count_monthly_fee_per_event_seq as running_count_monthly_fee_minus_running_count_monthly_fee_per_event_seq,
        coalesce(city, 'DIRECT') as city_filled
    from joined_subscriptions
    where city in ('Chiang Mai', 'Berlin', 'New York')
)
select
    id,
    city,
    running_count_monthly_fee,
    running_count_monthly_fee_per_event_seq_level,
    running_count_monthly_fee_per_event_seq_times_credit_limit,
    city_filled,
    case when city rlike '^(Tokyo|London)$' then 'OTHER_ZONE' when city rlike '^(Bangkok|Chiang Mai)$' then 'GROUP_A' else 'EXTENDED' end as city_zone
from valid_subscriptions;
