with top_accounts as (
    select *, row_number() over (partition by status order by opened_date, id) row_num
    from accounts
)
select
    id,
    status,
    opened_date,
    balance
from top_accounts
where row_num <= 3;
