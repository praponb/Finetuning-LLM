with not_flagged as (
    select s.*
    from accounts s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date
        )
)
select
    id,
    customer_id,
    account_type,
    balance,
    status,
    case when account_type rlike '^(CHECKING|LOAN)$' then 'GROUP_A' else 'OTHER_ZONE' end as account_type_zone
from not_flagged
order by status desc nulls last;
