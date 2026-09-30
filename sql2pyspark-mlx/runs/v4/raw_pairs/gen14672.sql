with cleaned as (
    -- Basic cleansing and defaults
    select *, cast(balance as int) as balance_int
    from accounts
    where opened_date >= date '2024-04-03'
      and status is not null
),
cleaned_accounts as (
    -- Basic cleansing and defaults
    select
        *,
        floor(balance_int) as balance_int_whole,
        add_months(opened_date, 6) as opened_date_plus_months,
        coalesce(status, 'OTHER') as resolved_status
    from cleaned
    where opened_date > to_date('2024-01-01')
      and status is not null
      and customer_id is not null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by status order by balance_int, id) as row_num
        from cleaned_accounts
    ) ranked
    where row_num = 1
)
select
    id,
    customer_id,
    account_type,
    balance,
    status,
    balance_int,
    balance_int_whole,
    opened_date_plus_months,
    row_num
from deduped
order by balance_int;
