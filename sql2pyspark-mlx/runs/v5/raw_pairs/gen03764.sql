with cleaned as (
    select
        *,
        add_months(opened_date, 12) as opened_date_plus_months,
        coalesce(status, 'UNKNOWN') as status_clean
    from accounts
    where status is not distinct from 'OPEN'
)
select *
from (
    select
        customer_id,
        status,
        account_type,
        balance
    from cleaned
) src
pivot (
    sum(balance) as value_sum,
    count(1) as n
    for account_type in ('CHECKING', 'SAVINGS')
)
