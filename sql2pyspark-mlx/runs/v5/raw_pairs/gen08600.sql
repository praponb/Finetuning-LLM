with matched_keys as (
    -- Set intersection on keys
    select account_type, customer_id
    from accounts
    intersect all
    select account_type, customer_id
    from accounts
    where customer_id = 18
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from accounts s
    inner join matched_keys k on s.account_type = k.account_type and s.customer_id = k.customer_id
)
select
    customer_id,
    balance,
    opened_date,
    status
from reconciled
order by balance
