with enriched as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over w_part as balance_percent_rank,
        lead(balance, 1) over (partition by status, account_type order by opened_date, id) as next_balance,
        row_number() over w_recent as seq_no
    from accounts
    window w_part as (partition by account_type order by balance desc nulls last), w_recent as (partition by customer_id order by opened_date desc, id)
)
select
    id,
    account_type,
    opened_date,
    status,
    next_balance,
    seq_no,
    case when status rlike '^(FROZEN|CLOSED)$' then 'OTHER_ZONE' else 'CORE' end as status_zone
from enriched
order by opened_date desc nulls last;
