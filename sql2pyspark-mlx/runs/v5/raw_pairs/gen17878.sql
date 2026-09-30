with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by status order by monthly_fee nulls last) as row_num
    from subscriptions
    qualify row_num = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        plan,
        start_date,
        status,
        row_num,
        ntile(10) over w_running as monthly_fee_bucket,
        row_number() over (partition by customer_id order by end_date, id) as row_seq
    from ranked
    where month(start_date) in (1, 2, 4)
    window w_running as (partition by customer_id order by monthly_fee, id)
),
with_dims as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.status,
        p.country,
        p.city
    from enriched src
    join customers p
        on src.customer_id <=> p.id
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        status,
        city,
        case when city in ('Bangkok', 'Singapore') then 'Group A' else 'Other' end as city_group,
        upper(status) as status_upper
    from with_dims
    where country is not null
      and status = 'EXPIRED'
),
enriched_subscriptions as (
    -- Attach customers attributes
    select
        s.*,
        d.segment,
        d.country,
        d.credit_limit
    from base s
    join customers d
        on s.customer_id = d.id
)
-- Aggregate with conditional metrics
select
    city,
    case when credit_limit >= 16100 then 'PREMIUM' else 'STANDARD' end as credit_limit_tier,
    max(credit_limit) as max_credit_limit,
    count(*) filter (where country = 'DE') as de_rows
from enriched_subscriptions
group by grouping sets ((city, case when credit_limit >= 16100 then 'PREMIUM' else 'STANDARD' end), (city), ())
having max_credit_limit > 0
order by city desc nulls first;
