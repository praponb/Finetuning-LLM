with base as (
    select
        id,
        account_type,
        balance,
        opened_date,
        status,
        case when status = 'FROZEN' then 1 else 0 end as is_frozen
    from accounts
    where opened_date >= date '2023-09-18'
      and status = 'CLOSED'
)
select
    *,
    avg(is_frozen) over (order by is_frozen, id rows between 3 preceding and current row) as is_frozen_avg_3,
    sum(balance) over (partition by account_type order by is_frozen, id rows between unbounded preceding and current row) as running_balance,
    lead(is_frozen, 1, 0) over (partition by account_type, status order by balance, id) as next_is_frozen
from base;
