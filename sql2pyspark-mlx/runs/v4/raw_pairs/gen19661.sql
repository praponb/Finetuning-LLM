with ValidAccounts as (
    select *
    from accounts
    where customer_id is not null and (account_type in ('CHECKING', 'CREDIT', 'LOAN') or opened_date > '2023-09-01')
),
summary as (
    select quarter(opened_date) opened_quarter, account_type, round((100 * sum(case when status = 'CLOSED' then 1 else 0 end)) / count(*), 1) pct_closed
    from ValidAccounts
    group by quarter(opened_date), account_type
)
select opened_quarter, account_type
from summary
where pct_closed > 40
order by opened_quarter desc, account_type
