with ranked as (
    select *, row_number() over (partition by account_id order by amount desc, id) pos
    from transactions
    where amount between 1500 and 3200
)
select id, account_id, amount
from ranked
where pos <= 3
order by amount desc;
