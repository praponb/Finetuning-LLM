with latest as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by plan, customer_id order by start_date desc nulls first) as row_num
    from subscriptions
    qualify row_num = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        first_value(monthly_fee, true) over (partition by customer_id order by monthly_fee, id rows between unbounded preceding and current row) as first_known_monthly_fee,
        first_value(monthly_fee) ignore nulls over (partition by plan order by start_date, id rows between unbounded preceding and current row) as first_known_monthly_fee_2,
        lag(monthly_fee, 1) over (partition by status order by start_date, id) as prev_monthly_fee,
        first_value(monthly_fee) ignore nulls over (order by end_date, id rows between unbounded preceding and current row) as first_known_monthly_fee_3
    from latest
    where end_date between '2024-09-24' and '2024-12-15'
),
base as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), start_date) as days_since_start_date,
        coalesce(first_known_monthly_fee_3, 0) as first_known_monthly_fee_3_filled
    from with_windows
    where start_date > date '2023-05-14'
      and customer_id is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(first_known_monthly_fee_3_filled) over (partition by status order by row_num, id rows between unbounded preceding and current row) as running_first_known_monthly_fee_3_filled,
        lead(days_since_start_date, 1) over (partition by plan order by row_num, id) as next_days_since_start_date
    from base
),
matched_keys as (
    -- Set difference on keys
    select status, customer_id
    from enriched
    except
    select status, customer_id
    from subscriptions
    where end_date < '2025-01-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join matched_keys k on s.status = k.status and s.customer_id = k.customer_id
)
select
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    row_num,
    prev_monthly_fee,
    first_known_monthly_fee_3,
    case when plan rlike '^(basic|premium)$' then 'GROUP_B' when plan rlike '^(standard|enterprise)$' then 'PRIORITY' else 'GROUP_A' end as plan_zone
from verified
order by customer_id desc nulls last
