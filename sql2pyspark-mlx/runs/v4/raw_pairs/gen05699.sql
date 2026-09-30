with ranked as (
    select *, row_number() over (partition by account_type, customer_id order by opened_date desc nulls first, id) as dedup_rank
    from accounts
    qualify dedup_rank = 1
)
select
    date_trunc('quarter', opened_date) as quarter_start,
    round(sum(balance) / nullif(count(case when account_type = 'CREDIT' then 1 end), 0), 2) as balance_per_credit,
    count(*) filter (where status = 'FROZEN') as frozen_rows,
    count(case when status = 'OPEN' then 1 end) as open_count
from ranked
group by date_trunc('quarter', opened_date)
order by balance_per_credit nulls last
