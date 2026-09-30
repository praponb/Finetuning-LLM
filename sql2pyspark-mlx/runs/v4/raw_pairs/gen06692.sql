with valid_accounts as (
    select *
    from accounts
    where account_type in ('LOAN', 'SAVINGS')
),
status_summary as (
    select status, min(opened_date) as min_opened_date, round((100 * sum(case when status = 'CLOSED' then 1 else 0 end)) / count(*), 1) as pct_closed
    from valid_accounts
    group by status
)
select status, min_opened_date, pct_closed
from status_summary
order by min_opened_date asc, status asc
limit 5
