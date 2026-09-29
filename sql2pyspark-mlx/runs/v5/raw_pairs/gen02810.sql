with base as (
    -- Keep recent, valid rows
    select *, length(plan) as plan_length
    from subscriptions
    where start_date > date '2023-04-14'
      and end_date < '2025-02-01'
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from base
    qualify row_number() over (partition by status order by plan_length, id) = 1
),
matched_keys as (
    -- Set difference on keys
    select status, plan
    from ranked
    except distinct
    select status, plan
    from subscriptions
    where start_date between '2023-06-01' and '2025-02-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    inner join matched_keys k on s.status = k.status and s.plan = k.plan
),
cleaned as (
    -- Keep recent, valid rows
    select *, greatest(plan_length, monthly_fee) as greatest_plan_length_monthly_fee
    from verified
    where customer_id in (7, 30, 32, 40)
)
select
    s.id,
    s.customer_id,
    s.plan,
    s.monthly_fee,
    s.status,
    s.greatest_plan_length_monthly_fee,
    (select max(o2.shipping_fee) from orders o2 where o2.customer_id = s.customer_id) as max_order_shipping_fee
from cleaned s
order by greatest_plan_length_monthly_fee nulls last, status;
