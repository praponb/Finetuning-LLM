with valid as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        add_months(end_date, 12) as end_date_plus_months
    from subscriptions
    where customer_id is not null
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by customer_id order by start_date desc nulls last, id asc) as rn
    from valid
    qualify rn = 1
),
deduped_subscriptions as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by customer_id, plan order by end_date_plus_months desc nulls first, id asc) as row_num
    from deduped
    qualify row_num = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by customer_id order by end_date desc, id asc) as row_seq,
        lag(monthly_fee, 2) over (partition by customer_id order by rn asc, id asc) as prev_monthly_fee
    from deduped_subscriptions
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        plan,
        prev_monthly_fee,
        monthly_fee
    from enriched
) src
unpivot (
    metric_value for metric_name in (prev_monthly_fee, monthly_fee as MONTHLY_FEE)
);
