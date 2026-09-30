with with_windows as (
    -- Running and moving aggregates
    select
        *,
        sum(balance) over (partition by account_type) as account_type_sum_balance,
        row_number() over (partition by customer_id order by opened_date desc, id) as event_seq,
        sum(balance) over (order by balance, id rows between unbounded preceding and current row) as cumulative_balance
    from accounts
)
select
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    account_type_sum_balance,
    cumulative_balance,
    case when status rlike '^(OPEN|CLOSED)$' then 'EXTENDED' when status rlike '^(FROZEN)$' then 'GROUP_A' else 'GROUP_B' end as status_zone
from with_windows
