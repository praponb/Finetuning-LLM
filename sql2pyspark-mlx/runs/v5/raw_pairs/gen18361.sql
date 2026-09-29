with TopAccounts as (
    select *, row_number() over (partition by status order by balance desc, id) as pos
    from accounts
)
select id, status, balance
from TopAccounts
where pos <= 1;
