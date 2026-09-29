with joined as (
    -- Attach customers attributes
    select
        src.*,
        c.signup_date,
        c.name
    from subscriptions src
    inner join customers c
        on src.customer_id = c.id
),
with_dims as (
    -- Attach customers attributes
    select
        a.*,
        c.credit_limit,
        c.age
    from joined a
    inner join customers c
        on a.customer_id = c.id
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by plan order by credit_limit desc, id) as dedup_rank
        from with_dims
    ) x
    where dedup_rank = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by status, plan order by age desc, id) as rn
        from ranked
    ) t
    where rn = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        signup_date,
        name,
        credit_limit,
        age,
        dedup_rank,
        initcap(trim(status)) as status_title
    from latest
    where start_date >= date '2024-02-29'
      and customer_id in (30, 32)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        ntile(10) over (partition by status order by credit_limit desc, id) as credit_limit_bucket,
        row_number() over (order by credit_limit, id) as event_seq,
        sum(credit_limit) over (partition by customer_id order by dedup_rank, id rows between unbounded preceding and current row) as running_credit_limit
    from cleaned
),
attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee as subscription_monthly_fee
    from with_windows a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.end_date between sub.start_date and sub.end_date
)
select
    id,
    customer_id,
    start_date,
    end_date,
    monthly_fee,
    status,
    name,
    age,
    dedup_rank
from attributed
order by dedup_rank nulls last;
