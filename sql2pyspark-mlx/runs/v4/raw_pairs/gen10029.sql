with matched_keys as (
    -- Set difference on keys
    select status
    from accounts
    except
    select status
    from accounts
    where customer_id in (30, 31, 36)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from accounts s
    join matched_keys k on s.status = k.status
)
select
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    case when status rlike '^(CLOSED|OPEN)$' then 'CORE' else 'OTHER_ZONE' end as status_zone
from kept_rows
order by balance desc;
