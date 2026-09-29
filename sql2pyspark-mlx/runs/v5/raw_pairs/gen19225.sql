with valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        datediff(current_date(), start_date) as days_since_start_date
    from subscriptions
    where end_date >= date '2023-08-01'
      and status in ('EXPIRED', 'PAUSED')
),
joined as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.start_date,
        s.monthly_fee,
        b.age
    from valid s
    left join customers b
        on s.customer_id <=> b.id
),
enriched as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        start_date,
        age,
        ntile(3) over (partition by customer_id order by age, id) as age_bucket,
        last_value(age) ignore nulls over (order by start_date, id rows between unbounded preceding and current row) as last_known_age,
        row_number() over w_ordered as event_seq
    from joined
    window w_ordered as (partition by customer_id order by age desc, id)
),
joined_subscriptions as (
    -- Attach customers attributes
    select s.*, d.city
    from enriched s
    left join customers d
        on s.customer_id <=> d.id
    where d.credit_limit > 10300
),
base as (
    -- Keep recent, valid rows
    select
        id,
        start_date,
        age,
        age_bucket,
        last_known_age,
        event_seq,
        city,
        round(last_known_age, 0) as last_known_age_rounded,
        coalesce(city, 'NONE') as resolved_city
    from joined_subscriptions
    where start_date >= to_date('2023-07-12')
      and city is distinct from 'Chiang Mai'
)
select
    id,
    start_date,
    age,
    event_seq
from base
order by event_seq nulls last;
