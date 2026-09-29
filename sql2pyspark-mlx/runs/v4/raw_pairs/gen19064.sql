with cleaned as (
    select *, last_day(opened_date) as opened_date_month_end
    from accounts
    where customer_id is not null
),
valid as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        add_months(opened_date_month_end, 6) as opened_date_month_end_plus_months,
        coalesce(status, 'OTHER') as resolved_status
    from cleaned
    where opened_date >= date '2023-09-30'
),
cleaned_accounts as (
    select
        *,
        datediff(current_date(), opened_date) as days_since_opened_date,
        date_add(opened_date, 5) as opened_date_plus_5d
    from valid
    where opened_date >= date '2024-02-28'
)
select
    date_trunc('month', opened_date_plus_5d) as activity_month,
    customer_id,
    count(*) as cnt,
    approx_percentile(balance, array(0.25, 0.5, 0.75)) as balance_quartiles,
    sum(case when account_type = 'CREDIT' then balance else 0 end) as credit_balance,
    size(collect_set(account_type)) as num_account_types,
    size(collect_set(account_type)) as num_account_types_2
from cleaned_accounts
group by date_trunc('month', opened_date_plus_5d), customer_id
order by activity_month desc nulls first
