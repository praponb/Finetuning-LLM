with joined as (
    -- Attach customers attributes
    select
        src.*,
        p.age,
        p.city
    from subscriptions src
    inner join customers p
        on src.customer_id = p.id
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        b.segment,
        b.name,
        b.signup_date
    from joined a
    inner join customers b
        on a.customer_id = b.id
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan as subscription_plan,
        sub.monthly_fee as subscription_monthly_fee
    from enriched a
    inner join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.signup_date between sub.start_date and sub.end_date
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by plan order by subscription_id desc, id, subscription_id) as rn
        from with_window
    ) ranked
    where rn = 1
),
enriched_subscriptions as (
    -- Attach customers attributes
    select
        src.*,
        c.credit_limit,
        c.city as customer_city
    from latest src
    inner join customers c
        on src.customer_id = c.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        city,
        segment,
        name,
        signup_date,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        rn,
        credit_limit,
        customer_city,
        upper(city) city_upper
    from enriched_subscriptions
    where city is not null
)
-- Grouped metrics
select
    date_trunc('quarter', signup_date) as quarter_start,
    size(collect_set(customer_city)) as num_customer_cities,
    min(credit_limit) as min_credit_limit,
    count(*) filter (where subscription_plan = 'premium') as premium_rows
from valid
group by quarter_start
order by quarter_start desc;
