with valid as (
    select
        id,
        customer_id,
        balance,
        opened_date,
        status,
        substring(account_type, 1, 3) as account_type_prefix,
        coalesce(status, 'N/A') as resolved_status
    from accounts
    where opened_date >= date '2024-04-16'
      and status is not null
)
select
    id,
    customer_id,
    balance,
    opened_date,
    status,
    account_type_prefix,
    resolved_status,
    row_number() over (partition by customer_id order by balance asc, id asc) as seq_no,
    cume_dist() over (partition by status, customer_id order by balance asc nulls last) as balance_cume_dist,
    row_number() over (order by balance asc, id asc) as event_seq
from valid
