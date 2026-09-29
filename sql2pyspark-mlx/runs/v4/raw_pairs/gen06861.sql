select
    id,
    customer_id,
    balance,
    opened_date
from (
    select
        id,
        customer_id,
        balance,
        opened_date,
        row_number() over (partition by customer_id order by balance desc, id) pos
    from accounts
    where account_type not in ('CREDIT', 'CHECKING')
) ranked
where pos <= 3
order by opened_date asc;
